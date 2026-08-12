#!/usr/bin/env python3
"""Bootstrap a Polaris test container: wait for the API, create the
warehouse bucket on the RUSTFS sidecar, create the catalog (dual endpoints:
the CLIENT reaches RUSTFS at 127.0.0.1, POLARIS reaches it through the
rootless-podman host alias), grant the admin role. Stdlib-only; test
tooling for this crate's container-backed suites — never product code.

Usage: polaris_bootstrap.py <polaris-api> <rustfs-client-endpoint>
         <rustfs-polaris-endpoint> <s3-access> <s3-secret>
         <client-id> <client-secret> <warehouse> <bucket>
"""
import datetime
import hashlib
import hmac
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

(
    polaris,
    s3_client_ep,
    s3_polaris_ep,
    s3_access,
    s3_secret,
    client_id,
    client_secret,
    warehouse,
    bucket,
) = sys.argv[1:10]
region = "us-east-1"


def wait_api():
    deadline = time.time() + 180
    url = f"{polaris}/api/catalog/v1/config?warehouse={warehouse}"
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except urllib.error.HTTPError:
            return  # any HTTP answer (401/400) means the API is up
        except Exception:
            time.sleep(0.5)
    sys.exit("polaris API never answered")


def sigv4_put_bucket():
    host = s3_client_ep.split("://", 1)[1]

    def sign(k, m):
        return hmac.new(k, m.encode(), hashlib.sha256).digest()

    for attempt in range(10):
        t = datetime.datetime.now(datetime.timezone.utc)
        amz, ds = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
        payload = hashlib.sha256(b"").hexdigest()
        creq = (
            f"PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{payload}\n"
            f"x-amz-date:{amz}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}"
        )
        scope = f"{ds}/{region}/s3/aws4_request"
        sts = (
            "AWS4-HMAC-SHA256\n" + amz + "\n" + scope + "\n"
            + hashlib.sha256(creq.encode()).hexdigest()
        )
        sig = hmac.new(
            sign(sign(sign(sign(("AWS4" + s3_secret).encode(), ds), region), "s3"), "aws4_request"),
            sts.encode(),
            hashlib.sha256,
        ).hexdigest()
        req = urllib.request.Request(
            f"{s3_client_ep}/{bucket}",
            method="PUT",
            headers={
                "x-amz-date": amz,
                "x-amz-content-sha256": payload,
                "Authorization": (
                    f"AWS4-HMAC-SHA256 Credential={s3_access}/{scope}, "
                    f"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"
                ),
            },
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            return
        except urllib.error.HTTPError as e:
            if e.code == 409:
                return
            last = f"HTTP {e.code}"
        except urllib.error.URLError as e:
            last = str(e.reason)
        time.sleep(0.5)
    sys.exit(f"create bucket failed: {last}")


def api(method, path, token=None, body=None, form=None, ok=(200, 201, 204, 409)):
    data = None
    headers = {}
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(polaris + path, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        if e.code not in ok:
            sys.exit(f"{method} {path}: HTTP {e.code}: {e.read()[:300]}")
        return e.code, b""


def main():
    wait_api()
    sigv4_put_bucket()
    _, raw = api(
        "POST",
        "/api/catalog/v1/oauth/tokens",
        form={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    )
    token = json.loads(raw)["access_token"]
    api(
        "POST",
        "/api/management/v1/catalogs",
        token=token,
        body={
            "catalog": {
                "name": warehouse,
                "type": "INTERNAL",
                "properties": {
                    "default-base-location": f"s3://{bucket}/warehouse",
                    # Vended to CLIENTS (the bench pipeline on the host):
                    "s3.endpoint": s3_client_ep,
                    "s3.path-style-access": "true",
                    "s3.access-key-id": s3_access,
                    "s3.secret-access-key": s3_secret,
                    "s3.region": region,
                    # The bench reset drops tables WITH purge between runs.
                    "polaris.config.drop-with-purge.enabled": "true",
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    # `endpoint` is what gets VENDED to clients (the host);
                    # `endpointInternal` is Polaris's own view of the same
                    # store (rootless-podman host alias) for metadata writes.
                    "endpoint": s3_client_ep,
                    "endpointInternal": s3_polaris_ep,
                    "pathStyleAccess": True,
                    "stsUnavailable": True,
                    "allowedLocations": [f"s3://{bucket}/warehouse"],
                },
            }
        },
    )
    api(
        "PUT",
        f"/api/management/v1/catalogs/{warehouse}/catalog-roles/catalog_admin/grants",
        token=token,
        body={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
    )
    api(
        "PUT",
        f"/api/management/v1/principal-roles/service_admin/catalog-roles/{warehouse}",
        token=token,
        body={"catalogRole": {"name": "catalog_admin"}},
    )
    print("polaris bootstrap complete")


if __name__ == "__main__":
    main()
