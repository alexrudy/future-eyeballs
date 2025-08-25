//! Happy Eyeballs algorithm for attempting a set of futures in parallel.
//!
//! This library provides a variant of a set of unordered futures which attempts
//! each with a delay between starting. The first successful future is returned.

use std::collections::VecDeque;
use std::future::IntoFuture;
use std::time::Instant;
use std::{fmt, future::Future, marker::PhantomData, time::Duration};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio::time::error::Elapsed;
use tracing::trace;

/// Error returned when the happy eyeballs algorithm finishes.
///
/// It contains the inner error if an underlying future errored
/// (this will always be the first error)
///
/// Otherwsie, the enum indicates what went wrong.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HappyEyeballsError<E> {
    /// The timeout was reached.
    Timeout(Duration),

    /// No progress can be made.
    NoProgress,

    /// An error occurred during the underlying future.
    Error(E),
}

impl<T> fmt::Display for HappyEyeballsError<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoProgress => write!(f, "no progress can be made"),
            Self::Error(e) => write!(f, "error: {e}"),
            Self::Timeout(d) => write!(f, "timeout: {}ms", d.as_millis()),
        }
    }
}

impl<T> std::error::Error for HappyEyeballsError<T>
where
    T: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Error(e) => Some(e),
            _ => None,
        }
    }
}

type HappyEyeballsResult<T, E> = Result<T, HappyEyeballsError<E>>;

/// Configuration for the happy eyeballs future collection.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct EyeballConfiguration {
    /// How long to wait before starting the next future.
    pub concurrent_start_delay: Option<Duration>,

    /// Overall algorithm timeout
    pub overall_timeout: Option<Duration>,

    /// How many futures to spawn at the beginning
    pub initial_concurrency: Option<usize>,

    /// The maximum number of simultaneous futures to spawn
    pub maximum_concurrency: Option<usize>,
}

/// Implements the Happy Eyeballs algorithm for connecting to a set of addresses.
///
/// This algorithm is used to connect to a set of addresses in parallel, with a
/// delay between each attempt. The first successful connection is returned.
///
/// When the `timeout` is not set, the algorithm will attempt to connect to only
/// one address at a time.
///
/// To connect to all addresses simultaneously, set the `timeout` to zero.
#[derive(Debug)]
pub struct EyeballSet<F, T, E> {
    queue: VecDeque<F>,
    tasks: FuturesUnordered<F>,
    config: EyeballConfiguration,
    started: Option<Instant>,
    error: Option<HappyEyeballsError<E>>,
    result: PhantomData<fn() -> T>,
}

impl<F, T, E> EyeballSet<F, T, E> {
    /// Create a new `EyeballSet` with an optional timeout.
    ///
    /// The timeout is the amount of time between individual connection attempts.
    pub fn new(configuration: EyeballConfiguration) -> Self {
        Self {
            queue: VecDeque::new(),
            tasks: FuturesUnordered::new(),
            config: configuration,
            started: None,
            error: None,
            result: PhantomData,
        }
    }

    /// Returns `true` if the set of tasks is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty() && self.queue.is_empty()
    }

    /// Returns the number of tasks in the set.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.tasks.len() + self.queue.len()
    }

    /// Push a future into the set of tasks.
    #[allow(dead_code)]
    pub fn push(&mut self, future: F)
    where
        F: Future<Output = std::result::Result<T, E>>,
    {
        self.queue.push_back(future);
    }
}

enum Eyeball<T> {
    Ok(T),
    Error,
    Exhausted,
}

impl<F, T, E> EyeballSet<F, T, E>
where
    F: Future<Output = Result<T, E>>,
{
    async fn join_next(&mut self) -> Eyeball<T> {
        self.started.get_or_insert_with(Instant::now);

        match self.tasks.next().await {
            Some(Ok(stream)) => Eyeball::Ok(stream),
            Some(Err(e)) if self.error.is_none() => {
                trace!("first attempt error");
                self.error = Some(HappyEyeballsError::Error(e));
                Eyeball::Error
            }
            Some(Err(_)) => {
                trace!("attempt error");
                Eyeball::Error
            }
            None => {
                trace!("exhausted attempts");
                Eyeball::Exhausted
            }
        }
    }

    async fn join_next_with_delay(&mut self) -> Result<Eyeball<T>, Elapsed> {
        if let Some(timeout) = self.config.concurrent_start_delay {
            tokio::time::timeout(timeout, self.join_next()).await
        } else {
            Ok(self.join_next().await)
        }
    }

    async fn process_all(&mut self) -> HappyEyeballsResult<T, E> {
        let mut initial_concurrency = (self.config.initial_concurrency.unwrap_or(self.queue.len()));
        if let Some(maximum_concurrency) = self.config.maximum_concurrency {
            initial_concurrency = initial_concurrency.min(maximum_concurrency);
        }

        for _ in 0..initial_concurrency {
            if let Some(future) = self.queue.pop_front() {
                self.tasks.push(future);
            }
        }

        loop {
            // Either the queue is empty, or we are already driving the maximum number of
            // concurrent futures, so all we can do is wait for the next future to finish.
            if self.queue.is_empty()
                || self
                    .config
                    .maximum_concurrency
                    .is_some_and(|c| c <= self.tasks.len())
            {
                match self.join_next().await {
                    Eyeball::Ok(outcome) => return Ok(outcome),
                    Eyeball::Error => continue,
                    Eyeball::Exhausted => {
                        return self
                            .error
                            .take()
                            .map(Err)
                            .unwrap_or(Err(HappyEyeballsError::NoProgress));
                    }
                }
            } else {
                // The queue isn't empty, and we can handle more concurrency.
                //
                // Wait until the next time we can spawn a new future.
                if let Ok(Eyeball::Ok(output)) = self.join_next_with_delay().await {
                    return Ok(output);
                }

                // Add another future to the set of futures we are driving.
                //
                // This is ok because we checked for the maximum concurrency condition above.
                debug_assert!(
                    self.config
                        .maximum_concurrency
                        .is_none_or(|c| c > self.tasks.len())
                );
                // Also okay because we checked if the queue was empty above.
                debug_assert!(!self.queue.is_empty());
                if let Some(future) = self.queue.pop_front() {
                    self.tasks.push(future);
                }
            }
        }
    }

    /// Finish the happy eyeballs algorithm, returning the first successful connection.
    pub async fn finish(&mut self) -> HappyEyeballsResult<T, E> {
        let result = match self.config.overall_timeout {
            Some(timeout) => tokio::time::timeout(timeout, self.process_all()).await,
            None => Ok(self.process_all().await),
        };

        match result {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(HappyEyeballsError::Timeout(
                self.started.unwrap_or_else(Instant::now).elapsed(),
            )),
        }
    }
}

/// Future returned by a happy-eyeball set waiting to finish.
///
/// An alternative to calling [`EyeballSet::finish`] with a named
/// future.
pub struct EyeballFuture<T, E>(BoxFuture<'static, HappyEyeballsResult<T, E>>);

impl<T, E> fmt::Debug for EyeballFuture<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("EyeballFuture").finish()
    }
}

impl<T, E> Future for EyeballFuture<T, E> {
    type Output = HappyEyeballsResult<T, E>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl<F, T, E> IntoFuture for EyeballSet<F, T, E>
where
    T: Send + 'static,
    E: Send + 'static,
    F: Future<Output = Result<T, E>> + Send + 'static,
{
    type Output = HappyEyeballsResult<T, E>;
    type IntoFuture = EyeballFuture<T, E>;

    fn into_future(mut self) -> Self::IntoFuture {
        EyeballFuture(Box::pin(async move { self.finish().await }))
    }
}

impl<F, T, E> Extend<F> for EyeballSet<F, T, E>
where
    F: Future<Output = Result<T, E>>,
{
    fn extend<I: IntoIterator<Item = F>>(&mut self, iter: I) {
        self.queue.extend(iter);
    }
}

#[cfg(test)]
mod tests {
    use std::future::Pending;
    use std::future::pending;
    use std::future::ready;

    use super::*;

    fn cfg_immediate() -> EyeballConfiguration {
        EyeballConfiguration {
            concurrent_start_delay: Some(Duration::ZERO),
            overall_timeout: Some(Duration::ZERO),
            ..Default::default()
        }
    }

    macro_rules! tokio_test {
        (async fn $fn:ident() { $($body:tt)+ }) => {
            #[test]
            fn $fn() {
                tokio::runtime::Builder::new_current_thread().enable_all()
                        .build()
                        .unwrap()
                        .block_on(async {
                            $($body)*
                        })
            }
        };
    }

    tokio_test! {
    async fn one_future_success() {
        let mut eyeballs = EyeballSet::new(cfg_immediate());

        let future = async { Ok::<_, String>(5) };

        eyeballs.push(future);

        assert!(!eyeballs.is_empty());

        let result = eyeballs.await;
        assert_eq!(result.unwrap(), 5);
    }}

    tokio_test! {
    async fn one_future_error() {
        let mut eyeballs: EyeballSet<_, (), &str> = EyeballSet::new(cfg_immediate());

        let future = async { Err::<(), _>("error") };

        eyeballs.push(future);

        let result = eyeballs.await;
        assert!(matches!(
            result.unwrap_err(),
            HappyEyeballsError::Error("error")
        ));
    }
    }

    tokio_test! {
    async fn one_future_timeout() {
        let mut eyeballs: EyeballSet<_, (), &str> = EyeballSet::new(cfg_immediate());

        let future = pending();
        eyeballs.push(future);

        let result = eyeballs.await;
        assert!(matches!(
            result.unwrap_err(),
            HappyEyeballsError::Timeout(_)
        ));
    }
    }

    tokio_test! {
    async fn empty_set() {
        let eyeballs: EyeballSet<Pending<Result<(), &str>>, (), &str> =
            EyeballSet::new(cfg_immediate());

        assert!(eyeballs.is_empty());
        let result = eyeballs.await;
        assert!(matches!(
            result.unwrap_err(),
            HappyEyeballsError::NoProgress
        ));
    }
    }

    tokio_test! {
    async fn multiple_futures_success() {
        let mut eyeballs = EyeballSet::new(cfg_immediate());

        let future1 = ready(Err::<u32, String>("error".into()));
        let future2 = ready(Ok::<_, String>(5));
        let future3 = ready(Ok::<_, String>(10));

        eyeballs.extend(vec![future1, future2, future3]);
        let result = eyeballs.await;

        assert_eq!(result.unwrap(), 5);
    }
    }

    tokio_test! {
    async fn multiple_futures_until_finished() {
        let mut eyeballs = EyeballSet::new(cfg_immediate());

        let future1 = ready(Err::<u32, String>("error".into()));
        let future2 = ready(Ok::<_, String>(5));
        let future3 = ready(Ok::<_, String>(10));

        eyeballs.push(future1);
        eyeballs.push(future2);
        eyeballs.push(future3);

        assert_eq!(eyeballs.len(), 3);

        let result = eyeballs.await;

        assert_eq!(result.unwrap(), 5);
    }
    }

    tokio_test! {
    async fn multiple_futures_error() {
        let mut eyeballs = EyeballSet::new(cfg_immediate());

        let future1 = ready(Err::<u32, &str>("error 1"));
        let future2 = ready(Err::<u32, &str>("error 2"));
        let future3 = ready(Err::<u32, &str>("error 3"));

        eyeballs.extend(vec![future1, future2, future3]);
        let result = eyeballs.await;

        assert!(matches!(
            result.unwrap_err(),
            HappyEyeballsError::Error("error 1")
        ));
    }
    }

    tokio_test! {
    async fn no_timeout() {
        let mut eyeballs = EyeballSet::new(Default::default());

        let future1 = ready(Err::<u32, &str>("error 1"));
        let future2 = ready(Err::<u32, &str>("error 2"));
        let future3 = ready(Err::<u32, &str>("error 3"));

        eyeballs.extend(vec![future1, future2, future3]);

        let result = eyeballs.await;

        assert!(matches!(
            result.unwrap_err(),
            HappyEyeballsError::Error("error 1")
        ));
    }
    }
}
