import base64
from dataclasses import dataclass, field
import datetime
import hashlib
import os
from typing import Literal, Optional
import uuid

import requests


@dataclass
class Line:
    Label: str
    Usage: float
    Read: float
    IsEstimated: bool
    MeterSerialNumberHis: str


@dataclass
class MeterUsage:
    IsError: bool
    IsDataAvailable: bool
    IsConsumptionAvailable: bool
    TargetUsage: float
    AverageUsage: float
    ActualUsage: float
    MyUsage: str  # so far have only seen 'NA'
    AverageUsagePerPerson: float
    IsMO365Customer: bool
    IsMOPartialCustomer: bool
    IsMOCompleteCustomer: bool
    IsExtraMonthConsumptionMessage: bool
    Lines: list[Line] = field(default_factory=list)
    AlertsValues: Optional[dict] = field(
        default_factory=dict
    )  # assumption that it could be a dict


@dataclass
class Measurement:
    hour_start: datetime
    usage: int  # Usage
    total: int  # Read


class ThamesWater:
    def __init__(
        self,
        email: str,
        password: str,
        account_number: int,
        client_id: str = "cedfde2d-79a7-44fd-9833-cae769640d3d",  # specific to Thames Water
    ):
        self.s = requests.session()
        self.account_number = account_number
        self.client_id = client_id

        self._authenticate(email, password)

    def _generate_pkce(self):
        self.pkce_verifier = (
            base64.urlsafe_b64encode(os.urandom(32)).decode("utf-8").rstrip("=")
        )
        self.pkce_challenge = (
            base64.urlsafe_b64encode(
                hashlib.sha256(self.pkce_verifier.encode()).digest()
            )
            .decode("utf-8")
            .rstrip("=")
        )

    def _authorize_b2c_1_tw_website_signin(self) -> tuple[str, str]:
        url = "https://login.thameswater.co.uk/identity.thameswater.co.uk/b2c_1_tw_website_signin/oauth2/v2.0/authorize"

        params = {
            "client_id": self.client_id,
            "scope": "openid profile offline_access",
            "response_type": "code",
            "redirect_uri": "https://www.thameswater.co.uk/login",
            "response_mode": "fragment",
            "code_challenge": self.pkce_challenge,
            "code_challenge_method": "S256",
            "nonce": str(uuid.uuid4()),
            "state": str(uuid.uuid4()),
        }

        r = self.s.get(url, params=params)
        r.raise_for_status()
        return dict(self.s.cookies)["x-ms-cpim-trans"], dict(self.s.cookies)[
            "x-ms-cpim-csrf"
        ]

    def _self_asserted_b2c_1_tw_website_signin(
        self, email: str, password: str, trans_token: str, csrf_token: str
    ):
        url = "https://login.thameswater.co.uk/identity.thameswater.co.uk/B2C_1_tw_website_signin/SelfAsserted"

        params = {
            "tx": f"StateProperties={trans_token}",
            "p": "B2C_1_tw_website_signin",
        }

        data = {"request_type": "RESPONSE", "email": email, "password": password}

        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "x-csrf-token": csrf_token,
        }

        r = self.s.post(url, params=params, data=data, headers=headers)
        r.raise_for_status()

    def _confirmed_b2c_1_tw_website_signin(self, trans_token: str, csrf_token: str):
        url = "https://login.thameswater.co.uk/identity.thameswater.co.uk/B2C_1_tw_website_signin/api/CombinedSigninAndSignup/confirmed"

        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        }

        params = {
            "rememberMe": "false",
            "tx": f"StateProperties={trans_token}",
            "csrf_token": csrf_token,
            "p": "B2C_1_tw_website_signin",
        }

        r = self.s.get(url, headers=headers, params=params)
        r.raise_for_status()

        confirmed_signup_structured_response = {
            item.split("=")[0]: item.split("=")[1]
            for item in r.url.split("#")[1].split("&")
        }
        return confirmed_signup_structured_response["code"]

    def _get_oauth2_code_b2c_1_tw_website_signin(self, confirmation_code: str):
        url = "https://login.thameswater.co.uk/identity.thameswater.co.uk/b2c_1_tw_website_signin/oauth2/v2.0/token"

        headers = {
            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        }

        data = {
            "client_id": self.client_id,
            "redirect_uri": "https://www.thameswater.co.uk/login",
            "scope": "openid offline_access profile",
            "grant_type": "authorization_code",
            "client_info": "1",
            "x-client-SKU": "msal.js.browser",
            "x-client-VER": "3.1.0",
            "x-ms-lib-capability": "retry-after, h429",
            "x-client-current-telemetry": "5|865,0,,,|,",
            "x-client-last-telemetry": "5|0|||0,0",
            "code_verifier": self.pkce_verifier,
            "code": confirmation_code,
        }

        r = self.s.post(url, headers=headers, data=data)
        r.raise_for_status()
        self.oauth_request_tokens = r.json()

    def _refresh_oauth2_token_b2c_1_tw_website_signin(self):
        url = "https://login.thameswater.co.uk/identity.thameswater.co.uk/b2c_1_tw_website_signin/oauth2/v2.0/token"

        data = {
            "client_id": self.client_id,
            "scope": "openid profile offline_access",
            "grant_type": "refresh_token",
            "client_info": "1",
            "x-client-SKU": "msal.js.browser",
            "x-client-VER": "3.1.0",
            "x-ms-lib-capability": "retry-after, h429",
            "x-client-current-telemetry": "5|61,0,,,|@azure/msal-react,2.0.3",
            "x-client-last-telemetry": "5|0|||0,0",
            "refresh_token": self.oauth_request_tokens["refresh_token"],
        }

        headers = {"content-type": "application/x-www-form-urlencoded;charset=utf-8"}

        r = self.s.get(url, headers=headers, data=data)
        r.raise_for_status()
        self.oauth_response_tokens = r.json()

    def _login(self, state: str, id_token: str):
        url = "https://myaccount.thameswater.co.uk/login"

        data = {
            "state": state,
            "id_token": id_token,
        }

        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "content-type": "application/x-www-form-urlencoded",
        }

        r = self.s.post(url, data=data, headers=headers)
        r.raise_for_status()

    def _authenticate(
        self,
        email: str,
        password: str,
    ):
        self._generate_pkce()
        trans_token, csrf_token = self._authorize_b2c_1_tw_website_signin()
        self._self_asserted_b2c_1_tw_website_signin(
            email, password, trans_token, csrf_token
        )
        confirmation_code = self._confirmed_b2c_1_tw_website_signin(
            trans_token, csrf_token
        )
        self._get_oauth2_code_b2c_1_tw_website_signin(confirmation_code)
        self._refresh_oauth2_token_b2c_1_tw_website_signin()

        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Referer": "https://myaccount.thameswater.co.uk/twservice/Account/SignIn?useremail=",
        }

        r = self.s.get("https://myaccount.thameswater.co.uk/mydashboard", headers=headers)
        r.raise_for_status()

        r = self.s.get(
            f"https://myaccount.thameswater.co.uk/mydashboard/my-meters-usage?contractAccountNumber={self.account_number}",
            headers=headers,
        )
        r.raise_for_status()

        r = self.s.get(
            "https://myaccount.thameswater.co.uk/twservice/Account/SignIn?useremail=",
            headers=headers,
        )
        r.raise_for_status()
        
        state = r.url.split("&state=")[1].split("&nonce=")[0].replace("%3d", "=")
        id_token = r.text.split("id='id_token' value='")[1].split("'/>")[0]
        self.s.get(r.url)
        self._login(state, id_token)
        self.s.cookies.set(name="b2cAuthenticated", value="true")

    def get_meter_usage(
        self,
        meter: int,
        start: datetime.datetime,
        end: datetime.datetime,
        granularity: Literal["H", "D", "M"] = "H",
    ) -> MeterUsage:
        url = "https://myaccount.thameswater.co.uk/ajax/waterMeter/getSmartWaterMeterConsumptions"

        params = {
            "meter": meter,
            "startDate": start.day,
            "startMonth": start.month,
            "startYear": start.year,
            "endDate": end.day,
            "endMonth": end.month,
            "endYear": end.year,
            "granularity": "H",
            "premiseId": "",
            "isForC4C": "false",
        }

        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Referer": "https://myaccount.thameswater.co.uk/mydashboard/my-meters-usage",
            "X-Requested-With": "XMLHttpRequest",
        }

        r = self.s.get(url, params=params, headers=headers)
        r.raise_for_status()

        data = r.json()
        data["Lines"] = [Line(**line) for line in data["Lines"]]
        return MeterUsage(**data)
