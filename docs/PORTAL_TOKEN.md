# Portal Token

### Getting Portal JWT token

- Login to
    - PROD: https://data.umccr.org
    - DEV: https://data.dev.umccr.org
- Click your username at top right corner > Token
- This will prompt token dialog. Click **COPY** button to copy in JWT token into your clipboard.
> 🙋‍♂️ WARNING: THIS IS YOUR PERSONAL ACCESS TOKEN (PAT). **YOU SHOULD NOT SHARE WITH ANY THIRD PARTY**.

### Exporting token

- Next, we will create system environment variable called `PORTAL_TOKEN`.
- For macOS/Linux user, do as follows:
```
export PORTAL_TOKEN=eyJraWQiOiJi<..shorten..for..brevity...>Ls4-2HTHSW2ohmQ
```
> 🙋‍♂️ If you'd like to do a quick detour about JWT token with R, check it out  [portal_decode.R](portal_decode.R)
