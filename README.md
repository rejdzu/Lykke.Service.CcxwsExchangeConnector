# Lykke.Service.CcxwsExchangeConnector

Exchange connector for ccxws library.

---

## Running Locally

Requires `node` and `npm`. Install all the needed packages with

```
npm install
```

For the adapter to work the `SettingsUrl` environment variable must point to a
JSON configuration. Locally it can point to a file. The following command can be
used to start the adapter:

```
# GNU/Linux or MacOS
SettingsUrl="settings.json" node app.js
```

```
# Windows
set SettingsUrl=settings.json
node app.js
```

## Running tests

To run the automated tests use:

```
node test
```
