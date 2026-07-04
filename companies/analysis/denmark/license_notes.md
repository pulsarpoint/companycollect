# License and access notes

CVR company data is publicly accessible for lookup through CVR.dk/DataCVR, and Erhvervsstyrelsen states that CVR data is publicly available. Public visibility should not be treated as permission for high-volume automated extraction or redistribution.

The official system-to-system API requires credentials and likely comes with access terms. Those terms were not available without entering the access process.

`cvrapi.dk` publishes custom terms effective from 2019. The terms allow copying, distribution, publication, modification, combination with other material, and commercial/non-commercial use. They prohibit charging for separate features such as name search or charging for showing information received from CVR API. They also prohibit bypassing the daily limit by rotating IP addresses or obscuring the User-Agent.

`cvrapi.dk` requires careful handling of advertising-protected companies. Information about advertising-protected companies must not be used for advertising contact, and downstream disclosure has specific declaration requirements under Danish CVR law.

Beneficial ownership data should be treated as restricted. Danish rules changed in 2025 so access to beneficial ownership information is limited to qualified users, with login or API access based on access category and declarations.

For no-auth collection, avoid:

- bypassing Cloudflare or other access controls
- bypassing `cvrapi.dk` daily limits
- collecting login-gated or beneficial ownership data
- redistributing raw public UI data without legal review
- using third-party APIs without checking commercial and redistribution terms
