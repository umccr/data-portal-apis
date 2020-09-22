# IAP OpenAPI Definitions

- We use [`libiap`](https://umccr-illumina.github.io/libiap/) SDK to take care of IAP integration for Portal and its pipeline automation.
- Just like `libiap`, we use these definitions with [Prism](https://github.com/stoplightio/prism) for local dev and integration test mock up. See docker compose [`iap-mock.yml`](../iap-mock.yml) stack.
- So, it is better sync these definitions from [libiap master tree](https://github.com/umccr-illumina/libiap/tree/master/swagger) or release version tag.
- They should get updated whenever we bump `libiap` version in [`requirements.txt`](../requirements.txt).
- At the mo, `libiap` is still private repo. So, just simply clone it elsewhere, checkout to master branch (or release version tag) and then copy all definitions and overwrite here.
