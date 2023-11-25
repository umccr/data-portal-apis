# Portal Token

### Getting Portal JWT token

- Login to
    - PROD: https://portal.umccr.org
    - STG: https://portal.stg.umccr.org
    - DEV: https://portal.dev.umccr.org
- Click your username at top right corner > Token
- This will prompt token dialog. Click **COPY** button to copy in JWT token into your clipboard.
- Token valid for 24 hours (1 day)

> WARNING: THIS IS YOUR PERSONAL ACCESS TOKEN (PAT). **YOU SHOULD NOT SHARE WITH ANY THIRD PARTY**.

### Exporting token

- Next, we will create system environment variable called `PORTAL_TOKEN`.
- For macOS/Linux user, do as follows:
```
export PORTAL_TOKEN=eyJraWQiOiJi<..shorten..for..brevity...>Ls4-2HTHSW2ohmQ
```

> If you'd like to do a quick detour about JWT token with R, check it out [portal_decode.R](examples/portal_decode.R)
