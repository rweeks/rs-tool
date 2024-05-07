# rs-tool: A Tool for Reservoir Sampling

`rs-tool` processes a log file or a stream of line-delimited records from `stdin`. It uses [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) to produce a sample of its input on a per-record or per-field basis. It prints its output to `stdout` in either tabular or JSON format.

Given a suitable log file, you can use `rs-tool` to answer questions like:
- what are the most common IP addresses that access my web site?
- which users use the `sudo` command the most?
- what are the busiest times of day for my service?

When `rs-tool` reads its input from a file, it uses the [`Rayon` parallelism library](https://docs.rs/rayon/latest/rayon/) to construct and merge reservoirs in parallel.

Inspired by [Tim Bray's `tf`](https://github.com/timbray/topfew).
