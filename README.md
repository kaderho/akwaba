# akwaba — CI-EC Baseline Contracts

Référentiel des contrats d'intégration pour le système d'**état civil ivoirien (CI-EC)**.

> Architecture : **CQRS strict** · **Kafka + Protobuf** · **Confluent Schema Registry (SR2)** · **MDM1/MDMV1**

---

## Contenu du dépôt

| Répertoire / Fichier | Description |
|----------------------|-------------|
| `proto/` | Contrats Protobuf — un fichier `XxxEvent` par domaine |
| `openapi/gateway-v1.yaml` | Spec OpenAPI 3.1 de la gateway API (API1) |
| `mdm/` | Fichiers MDM (référentiels de codes) + documentation |
| `docs/architecture.md` | Norme d'intégration (FR) |
| `scripts/` | Scripts lint/build Protobuf |
| `buf.yaml` | Configuration buf (lint + breaking change) |
| `buf.gen.yaml` | Configuration génération de code buf |

---

## Schémas Protobuf (proto/)

```
proto/
├── common/
│   ├── event_envelope.proto   # EventEnvelope (métadonnées Kafka)
│   └── code_types.proto       # CodeRef (MDM MDMV1)
├── mdm/                       # ci.ec.mdm.events.v1
├── person/                    # ci.ec.person.events.v1
├── civilrecord/               # ci.ec.civil_record.events.v1
├── mention/                   # ci.ec.mention.events.v1 + mention_set
├── document/                  # ci.ec.document.events.v1
├── request/                   # ci.ec.request.events.v1
├── extract/                   # ci.ec.extract_issuance.events.v1
├── registry/                  # ci.ec.registry.events.v1 + annual_table
├── consular/                  # ci.ec.consular_record.events.v1
├── central/                   # ci.ec.central_record.events.v1
├── foreign/                   # ci.ec.foreign_act.events.v1
├── payment/                   # ci.ec.payment.events.v1
├── access/                    # ci.ec.access.events.v1
└── audit/                     # ci.ec.audit.events.v1
```

Chaque topic Kafka possède **un seul** message racine `XxxEvent` :
```protobuf
message CivilRecordEvent {
  ci.ec.common.v1.EventEnvelope meta = 1;
  oneof payload { ... }
}
```

---

## Décisions d'architecture

| Décision | Choix |
|----------|-------|
| Architecture | CQRS strict |
| Transport événements | Kafka + Protobuf |
| Schema Registry | Confluent SR2 — TopicNameStrategy — BACKWARD |
| MDM | MDM1 avec MDMV1 (CodeRef.version obligatoire dans les snapshots) |
| Codes lieux | P1 (format `CI-<region>-<seq>`) |
| Types d'extraits | E2 (intégral naissance), V1 (filiation), M2 (mariage), H2 (décès) |
| Code de vérification | VC1 — Base32 Crockford 14 chars (HMAC-SHA256) |
| Gateway API | API1 — pattern C1 (HTTP 202 + correlationId) |

---

## Étapes de validation

### Prérequis
- [buf](https://buf.build/docs/installation) ≥ 1.28
- `git`

### 1. Lint des fichiers Protobuf
```bash
./scripts/lint-protos.sh
# ou directement :
buf lint
```

### 2. Vérification des breaking changes
```bash
buf breaking --against ".git#branch=origin/master"
```

### 3. Build / génération de code (optionnel)
```bash
./scripts/build-protos.sh
# Sortie Go  : gen/go/
# Sortie Java: gen/java/  (si plugin Java activé dans buf.gen.yaml)
```

### 4. Validation OpenAPI
```bash
# Avec Redocly CLI :
npx @redocly/cli lint openapi/gateway-v1.yaml

# Avec Spectral :
npx @stoplight/spectral-cli lint openapi/gateway-v1.yaml
```

### 5. Vérifier le code de vérification extrait (VC1)
```
GET https://api.etatcivil.ci/v1/query/verify?code=3F6K9D2PX7QJM8
```

---

## Références

- [docs/architecture.md](docs/architecture.md) — Norme d'intégration complète (FR)
- [mdm/README.md](mdm/README.md) — Documentation MDM
- [openapi/gateway-v1.yaml](openapi/gateway-v1.yaml) — Spec OpenAPI Gateway
