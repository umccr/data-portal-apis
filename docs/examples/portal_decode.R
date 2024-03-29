# -*- coding: utf-8 -*-
# JWT Decoder in R
# Copyright (c) 2021 UMCCR <services@umccr.org>
# MIT License
# https://opensource.org/licenses/MIT
#
# How to run:
#   export PORTAL_TOKEN=<your_token>
#   Rscript portal_decode.R

depedencies <- list("openssl", "jose", "jsonlite", "anytime")
invisible(lapply(depedencies, function (d) if (!require(d, character.only = TRUE)) install.packages(d, repos = "https://cran.ms.unimelb.edu.au")))

library(openssl)
library(jose)
library(jsonlite)
library(anytime)

token <- Sys.getenv("PORTAL_TOKEN")

# JWT format has 3 parts: header, body and signature that is separated by period "." character
token_chunks <- strsplit(token, ".", fixed = TRUE)[[1]]
# token_chunks

# ---

cat("\n")
header <- token_chunks[1]
# cat(sprintf("Header (e):\t%s\n", header))
cat(sprintf("Header (d):\t%s\n", rawToChar(base64url_decode(header))))
hdoc <- fromJSON(txt = rawToChar(base64url_decode(header)))
# hdoc
# cat(sprintf("kid       :\t%s\n", hdoc$kid))
# cat(sprintf("alg       :\t%s\n", hdoc$alg))

# ---

cat("\n")
body <- token_chunks[2]
# cat(sprintf("Body (e):\t%s\n", body))
cat(sprintf("Body (d):\t%s\n", rawToChar(base64url_decode(body))))

cat("\n")
doc <- fromJSON(txt = rawToChar(base64url_decode(body)))
# doc
cat(sprintf("Issues  At:\t%s\n", anytime(doc$iat)))
cat(sprintf("Expires At:\t%s\n", anytime(doc$exp)))

# ---

# So far, in above sections, we just simply decode Base64 of "unverified" JWT token claim.
# Typically we should trust the claim, only if we can verify the singed signature.
# We need issurer public key to decode and verify the signature. If this is success then trust the body content.
# Public key should typically "publically" available at issurer (iss) well known location such as:
#  {iss}/.well-known/jwks.json
#  {iss}/.well-known/openid-configuration
#
# For Cognito User Pool, it is located at
#   https://cognito-idp.{region}.amazonaws.com/{userPoolId}/.well-known/jwks.json
# hence, we can construct {iss}/.well-known/jwks.json
# More about JWKS? See https://auth0.com/docs/tokens/json-web-tokens/json-web-key-sets

# cat("\n")
# cat(sprintf("Issurer:\t%s\n", doc$iss))
# signature <- token_chunks[3]
# cat(sprintf("Signature (e):\t%s\n", signature))
#
# cat("\n")
# jwks_endpoint <- paste0(doc$iss, "/.well-known/jwks.json")
# cat(sprintf("Cognito JWKS:\t%s\n", jwks_endpoint))
#
# cat("\n")
# cat(sprintf("Verified Claim:\n------------------\n\n"))
# jwks <- fromJSON(jwks_endpoint)
# selected_key <- jwks$keys[jwks$keys$kid == hdoc$kid,]
# public_key <- read_jwk(selected_key)
# verified_claim <- jwt_decode_sig(token, pubkey = public_key)
# verified_claim
# identical(doc$exp, verified_claim$exp)
