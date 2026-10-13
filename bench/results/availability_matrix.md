| operation | consistency | 0 down | 1 down | 2 down |
|---|---|---|---|---|
| gRPC PutTick | ONE | 5/5 ok | 5/5 ok | 5/5 ok |
| gRPC PutTicks | ONE | 5/5 ok | 5/5 ok | 5/5 ok |
| gRPC GetTicks | ALL | 5/5 ok | UNAVAILABLE (0/5 ok) | UNAVAILABLE (0/5 ok) |
| REST PUT record | QUORUM | 5/5 ok | 5/5 ok | 503 (0/5 ok) |
| REST GET records | QUORUM | 5/5 ok | 5/5 ok | 503 (0/5 ok) |
| REST POST company | QUORUM+SERIAL | 5/5 ok | 5/5 ok | 503 (0/5 ok) |
| REST GET companies | QUORUM | 5/5 ok | 5/5 ok | 503 (0/5 ok) |
