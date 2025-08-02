# Happy Eyeballs Algorithm for futures

The Happy Eyeballs Algorithm is an algorithm developed in the context of IP connections by the IETF for racing connections via different protocols and addresses. Discussed in [RFC 6555](https://datatracker.ietf.org/doc/html/rfc6555) and later [RFC 8503](https://datatracker.ietf.org/doc/html/rfc8305), it provides a simple algorithm for racing connections to multiple destination hosts with timeouts and delays to prevent over-saturating a single host.

Although the algorithm is explicitly defined in the IPv4/IPv6 and TCP contexts, it can be implemented to race futures of any sort while maintaining both a delayed start for subsequent attempts (to prevent overwhelming some other resource) and first-successful-future wins semantics.

Often happy-eyeballs refers to only racing between the IPv4 and IPv6 connections. This implementation doesn't make that distinction as it is indifferent to the purpose of the underlying futures. If you want to properly obey happy-eyeballs by only connecting to the first availalbe address on each protocol, you would only submit 2 futures to this implementation, one for IPv6 and one for IPv4.

## Async Runtime Support

Currently, this implementation bakes in Tokio support, but only for the use of timeouts, and that would be pretty easy to extract if someone wanted to do the work.
