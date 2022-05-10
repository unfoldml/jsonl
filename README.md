# jsonl

Adapters between 'aeson' and the JSONL format (https://jsonlines.org/)

The JSONL format is best suited for encoding datasets made of structured objects, safer to parse than CSV and a lighter-weight alternative to a document database.

We provide a basic in-memory interface (the `jsonl` package ![](https://img.shields.io/hackage/v/jsonl.svg https://hackage.com/package/jsonl)  ) and a streaming one, suitable for large datasets (`jsonl-conduit`).
