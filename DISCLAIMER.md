# Disclaimer

## 1. No Warranty

This software is provided "as is", without warranty of any kind, express or
implied, including but not limited to the warranties of merchantability,
fitness for a particular purpose, title and non-infringement. Use of this
software development kit (the "SDK") is at your own risk.

## 2. Not Financial, Investment, Legal or Tax Advice

The SDK is a client library that provides programmatic access to Kraken's
public application programming interface ("API"). It does not provide financial
advice, investment advice, trading recommendations, portfolio guidance, legal
advice or tax advice, and nothing in this repository — including code,
comments, documentation, examples or sample strategies — is intended or should
be construed as any of those things.

Any example code in this repository exists to illustrate how to call the API.
Examples are not trading strategies, are not tested for profitability, and are
not recommendations to enter into any transaction. All trading and investment
decisions you make using the SDK are yours alone.

## 3. Live Endpoints, Real Money, Real Consequences

Authenticated methods in this SDK reach the live Kraken API and can create
real, binding financial transactions on a real account. Orders, cancellations,
transfers and withdrawals may be irreversible once submitted and processed.
Incorrect parameters, unhandled errors, logic bugs in your own code, bugs in
this SDK, network failures, and unexpected API responses can each result in
unintended transactions and financial loss.

Before running code that uses authenticated endpoints against a funded account:

- Read the API documentation for each endpoint you call, including its
  parameter semantics and error responses.
- Use an API key scoped to the minimum permissions your use case requires.
- Test against small amounts first.
- Implement your own validation, error handling, retry limits and kill
  switches.

This SDK does not supply them, does not validate whether an instruction is
economically sensible, and does not second-guess the calls you make.

## 4. API Key Security

Your API key and secret grant access to your Kraken account. Treat them like
passwords.

- Never commit them to source control, and never include them in logs,
  screenshots, bug reports, issues or chat messages.
- Never hard-code them. Load them from environment variables or a secrets
  manager.
- Grant each key only the permissions it needs. Do not enable withdrawal
  permissions unless your application requires them.
- Rotate keys regularly and revoke keys that are no longer in use.

Anything with access to your process environment — a shared host, a continuous
integration runner, a co-resident dependency, an artificial intelligence coding
agent or automated tool with access to your machine or repository — may be able
to read credentials available to that process. Scope and isolate accordingly.

You are responsible for all activity conducted using your credentials, whether
initiated by you, by your code, or by any automated or agentic system you grant
access to them.

## 5. No Commitment as to the API or the SDK

Kraken's API may change, and endpoints, parameters, response formats, rate
limits and availability may be added, altered, deprecated or withdrawn. Kraken
gives no undertaking that this SDK will remain compatible with the API, that it
will be maintained or updated, that any particular version or language binding
will continue to function or continue to be published, or that it covers the
full API surface.

Always validate against the current published API documentation, which governs
in the event of any inconsistency with this SDK.

## 6. Availability of Kraken Products and Services

The availability of Kraken products, services, assets and trading pairs varies
by jurisdiction, by the Kraken entity with which you contract, and by your
client classification. The presence of a method, parameter, asset or endpoint
in this SDK does not mean the corresponding product or service is available to
you, and nothing in this repository is an offer, solicitation, invitation or
recommendation to buy, sell or transact in any asset, or to use any Kraken
product or service, in any jurisdiction where doing so would be unlawful or
where Kraken is not authorised to offer it.

Your use of the Kraken API and of any Kraken service remains governed by the
applicable Kraken terms of service and API terms, and by applicable law. You
are responsible for your own compliance, including any licensing, registration,
market conduct, tax and reporting obligations that apply to your activity.

## 7. Forks, Modifications and Third-Party Distributions

This SDK is open source. Third parties may copy and modify it. Kraken has no
responsibility for, and does not endorse, review or support, any fork, modified
version, repackaged distribution or derivative work, whether or not it carries
the Kraken name. Only releases published by Kraken from this repository are
Kraken's.

The open source licence covering this SDK does not grant any right to use the
Kraken or Payward names, logos, or other trade marks. See `TRADEMARK.md`.

## 8. Limitation of Liability

To the maximum extent permitted by applicable law, Payward, Inc., its
affiliates, and the authors and contributors to this SDK accept no liability
for any loss or damage — including financial loss, trading losses, missed or
erroneous executions, lost profits, loss of data, business interruption, or any
indirect, incidental, special, consequential or punitive damages — arising out
of or in connection with the SDK or its use, whether used manually,
programmatically, or by any automated or agentic system, and whether or not
Kraken has been advised of the possibility of such loss.

Nothing in this disclaimer excludes or limits any liability that cannot lawfully
be excluded or limited.

## 9. Security Reporting and Support

This repository is not within the scope of Kraken's bug bounty programme. Bug
reports and feature requests for the SDK are handled through GitHub Issues.
GitHub Issues is not a support channel for Kraken accounts, and Kraken gives no
undertaking as to response or resolution. For account, funding or trading
support, visit [support.kraken.com](https://support.kraken.com).

If you believe you have found a security vulnerability in Kraken's production
systems (as distinct from this SDK), report it through Kraken's security
disclosure process and not through a public GitHub issue.

## 10. Third-Party Software

This SDK incorporates third-party open source components. The components
included in this package, and the licences under which they are provided, are
listed in the `THIRD_PARTY_NOTICES.html` file shipped with each language binding
(for Rust, `rust/THIRD_PARTY_NOTICES.html`). Those components are licensed to
you by their respective authors on the terms set out there, and not by Kraken.

## 11. License

This SDK is open-sourced by Payward, Inc. (Kraken) under the license set out in
[LICENSE](LICENSE).
