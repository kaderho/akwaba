# Architecture CI-EC — Norme d'intégration (v1)

> Document normatif — langue : Français  
> Dernière mise à jour : 2026-04

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Principes CQRS](#2-principes-cqrs)
3. [Kafka et Protobuf](#3-kafka-et-protobuf)
4. [Schema Registry (SR2 — Confluent)](#4-schema-registry-sr2--confluent)
5. [MDM et règle MDMV1](#5-mdm-et-règle-mdmv1)
6. [Codes lieux (P1)](#6-codes-lieux-p1)
7. [Extraits (E2, V1, M2, H2) et code de vérification (VC1)](#7-extraits-e2-v1-m2-h2-et-code-de-vérification-vc1)
8. [Gateway API (API1) et pattern commande C1](#8-gateway-api-api1-et-pattern-commande-c1)
9. [Idempotence et corrélation](#9-idempotence-et-corrélation)
10. [Replay et Dead Letter Queue (DLQ)](#10-replay-et-dead-letter-queue-dlq)
11. [Topics Kafka et sujets Schema Registry](#11-topics-kafka-et-sujets-schema-registry)
12. [Conventions event_type](#12-conventions-event_type)
13. [Compatibilité Protobuf](#13-compatibilité-protobuf)

---

## 1. Vue d'ensemble

Le système **CI-EC** (État Civil Ivoirien) est une architecture de microservices construite sur :

- **CQRS strict** : séparation totale des flux d'écriture (commandes) et de lecture (requêtes).
- **Event Sourcing partiel** : les événements Kafka sont la source de vérité pour la réplication entre services.
- **Kafka + Protobuf** : transport des événements de domaine, fiable et schématisé.
- **Confluent Schema Registry (SR2)** : gouvernance des schémas Protobuf, compatibilité BACKWARD.
- **MDM CI-EC (MDM1)** : référentiel centralisé des codes métier, avec versionnement MDMV1.
- **Gateway API (API1)** : point d'entrée HTTP unique, pattern commande C1 (202 + correlationId).

```
┌─────────────────────────────────────────────────────────────────────┐
│                         API Gateway (API1)                          │
│               /commands/*              /query/*                     │
└────────────────────┬──────────────────────────┬────────────────────┘
                     │ HTTP 202 + correlationId  │ HTTP 200
                     ▼                           ▼
          ┌──────────────────┐       ┌──────────────────┐
          │  Write Services  │       │  Read Services   │
          │  (CQRS Command)  │       │  (CQRS Query)    │
          └────────┬─────────┘       └──────────────────┘
                   │ Kafka Events (Protobuf)         ▲
                   ▼                                 │
          ┌──────────────────┐       ┌──────────────────┐
          │   Kafka Broker   │──────▶│  Read Projectors │
          │ + Schema Registry│       │  (read models)   │
          └──────────────────┘       └──────────────────┘
```

---

## 2. Principes CQRS

### 2.1 Écriture (Command side)
- Toute mutation passe par une **commande HTTP** vers `/commands/*` (voir Gateway API1).
- Le write-service valide la commande, publie un événement Kafka et répond **202 Accepted** immédiatement.
- **Aucun** write-service n'expose de lecture de son état interne.

### 2.2 Lecture (Query side)
- Les read-services consomment les événements Kafka et maintiennent des **projections** (read models).
- Toute lecture passe par `/query/*`.
- La cohérence est **éventuelle** (eventual consistency).

### 2.3 Règle stricte
> Un service ne peut pas appeler un autre service en écriture **et** lire son état dans la même transaction.  
> Toute communication inter-services se fait via **événements Kafka**.

---

## 3. Kafka et Protobuf

### 3.1 Encodage des messages
| Partie | Encodage |
|--------|----------|
| **Key** | UTF-8 — UUID ou code composite (ex: `civil_record_id`) |
| **Value** | Protobuf sérialisé — message racine du topic (ex: `CivilRecordEvent`) |
| **Headers** | UTF-8 — voir section 3.2 |

### 3.2 Headers Kafka obligatoires
Même avec Schema Registry, les headers suivants DOIVENT être présents pour faciliter le debug et l'audit sans décodage Protobuf :

| Header | Exemple | Description |
|--------|---------|-------------|
| `event_id` | `550e8400-e29b-41d4-a716-446655440000` | UUID v4 unique |
| `event_type` | `ci.ec.civil_record.CivilRecordCreated` | Voir convention section 12 |
| `correlation_id` | `<uuid>` | Transmis du gateway au consumer |
| `producer_service` | `civil-record-service` | Nom du service producteur |
| `producer_version` | `1.4.2` | Version du service |

### 3.3 Structure d'un message Kafka
Chaque topic possède **un seul** message racine `XxxEvent` contenant :
- `EventEnvelope meta` — métadonnées communes
- `oneof payload` — événement métier spécifique

```protobuf
message CivilRecordEvent {
  ci.ec.common.v1.EventEnvelope meta = 1;
  oneof payload {
    CivilRecordCreated     created     = 10;
    CivilRecordAmended     amended     = 11;
    CivilRecordAnnotated   annotated   = 12;
    CivilRecordCancelled   cancelled   = 13;
    CivilRecordTransferred transferred = 14;
  }
}
```

---

## 4. Schema Registry (SR2 — Confluent)

### 4.1 Configuration globale
| Paramètre | Valeur |
|-----------|--------|
| Implémentation | Confluent Schema Registry |
| Subject name strategy | `TopicNameStrategy` |
| Compatibilité par défaut | `BACKWARD` |
| Format | Protobuf |

### 4.2 Convention de nommage des sujets
```
<topic_name>-value
```
Exemples :
- topic `ci.ec.civil_record.events.v1` → sujet `ci.ec.civil_record.events.v1-value`
- topic `ci.ec.extract_issuance.events.v1` → sujet `ci.ec.extract_issuance.events.v1-value`

### 4.3 Règles de compatibilité BACKWARD
- Les consumers peuvent lire les **anciens** messages avec les **nouveaux** schémas.
- **Autorisé** : ajouter de nouveaux champs (optionnels).
- **Interdit** : renommer / supprimer des champs, réutiliser des numéros de tags.
- **Interdit** : renommer / renuméroter les valeurs d'enum (sauf ajout en fin).

### 4.4 CI/CD Schema Registry
```bash
# Vérifier la compatibilité avant déploiement
buf breaking --against .git#branch=master

# Enregistrer les schémas (après validation)
# (utiliser le client confluent ou l'API REST du registry)
```

---

## 5. MDM et règle MDMV1

### 5.1 Principe MDMV1
Chaque `CodeRef` dans un **snapshot d'événement** DOIT inclure la version MDM :

```json
{ "system": "CI-EC-MDM", "code": "SEX.MALE", "version": "2026-04" }
```

### 5.2 Règles write-service
Pour chaque commande qui produit un snapshot :
1. Valider que le `code` existe dans le codeset correspondant (version active).
2. Si `version` est absente dans la commande → assigner `version = active_version_at(occurred_at)`.
3. Si `version` est présente (cas admin) → vérifier qu'elle est `PUBLISHED` et dans la plage autorisée.
4. Enregistrer la version injectée dans les logs d'audit.

### 5.3 Pourquoi MDMV1 ?
- **Audit-proof** : chaque événement sait avec quelle version MDM il a été produit.
- **Reproductibilité** : rejouer un événement donne exactement le même résultat.
- **Compatible** avec les extraits E2/V1/M2/H2 : la version MDM est figée dans le snapshot.

---

## 6. Codes lieux (P1)

Les codes lieux suivent le format :
```
<ISO-country>-<region-abbr>-<sequence>
```
Exemple : `CI-AB-001` = Abidjan – Plateau (Côte d'Ivoire, région Lagunes)

Le référentiel complet est dans `mdm/ci-ec-mdm-place-2026-04.yaml`.

---

## 7. Extraits (E2, V1, M2, H2) et code de vérification (VC1)

### 7.1 Types d'extraits
| Code MDM | Description |
|----------|-------------|
| `EXTRACT.KIND.E2` | Extrait intégral d'acte de naissance |
| `EXTRACT.KIND.V1` | Extrait avec filiation |
| `EXTRACT.KIND.M2` | Copie intégrale d'acte de mariage |
| `EXTRACT.KIND.H2` | Extrait d'acte de décès |

### 7.2 Code de vérification (VC1)
- **Algorithme** : HMAC-SHA256(secret, `issuance_id || issued_at || civil_record_id || version`)
- **Encodage** : Base32 Crockford (sans I, L, O, U) — 13 chars données + 1 char checksum = **14 chars**
- **Présentation** : groupée pour l'humain (ex: `3F6K-9D2P-X7QJ-M8`), stockée sans tirets
- **Format MDM** : `VERIFICATION.CODE.BASE32_CROCKFORD_V1`

### 7.3 Vérification d'un extrait
- Endpoint public : `GET /query/verify?code=<14chars>`
- Retourne uniquement : `valid`, `issuedAt`, `kind`, `deliveredByOfficeId`, `targetCivilRecordType`
- **Pas de données nominatives** (privacy by design).

---

## 8. Gateway API (API1) et pattern commande C1

### 8.1 Pattern C1
```
POST /commands/<resource>
→ 202 Accepted
  { "correlationId": "<uuid>", "message": "Command accepted" }
```

- La commande est publiée de manière asynchrone sur Kafka.
- Le client suit l'état via `/query/<resource>/<id>` ou via webhooks.

### 8.2 Transmission du correlationId
Le `correlationId` est transmis dans :
1. La réponse HTTP 202
2. L'`EventEnvelope.correlation_id` dans le message Kafka
3. Le header Kafka `correlation_id`
4. Tous les événements dérivés (chaîne causale)

### 8.3 Authentification
- Toutes les routes `/commands/*` et `/query/*` requièrent un JWT Bearer.
- Exception : `GET /query/verify` est public (vérification d'extrait).

---

## 9. Idempotence et corrélation

### 9.1 Idempotence des commandes
- Chaque commande DOIT contenir un `commandId` (UUID v4) côté client.
- Le write-service DOIT déduplicer sur `commandId` (fenêtre minimale : 24h).
- Si un `commandId` déjà vu est reçu → répondre 202 avec le même `correlationId`.

### 9.2 Idempotence des consumers
- Chaque consumer Kafka DOIT être idempotent.
- Utiliser l'`event_id` de l'`EventEnvelope` comme clé de déduplication.
- En cas de re-livraison, le résultat DOIT être identique.

---

## 10. Replay et Dead Letter Queue (DLQ)

### 10.1 Dead Letter Queue
- Topic DLQ : `<topic_original>.dlq`  
  Exemple : `ci.ec.civil_record.events.v1.dlq`
- Un message va en DLQ après N tentatives (N configurable, défaut = 3).
- Les messages DLQ contiennent les headers originaux + `x-error-message`, `x-retry-count`.

### 10.2 Replay
- Le replay se fait depuis le début du topic (offset 0) ou depuis un offset/timestamp précis.
- Les consumers idempotents (section 9.2) garantissent la sécurité du replay.
- Ordre de replay recommandé :
  1. `ci.ec.mdm.events.v1` (référentiels en premier)
  2. `ci.ec.person.events.v1`
  3. `ci.ec.civil_record.events.v1`
  4. Autres domaines

---

## 11. Topics Kafka et sujets Schema Registry

| Topic Kafka | Sujet SR2 | Message racine |
|-------------|-----------|----------------|
| `ci.ec.mdm.events.v1` | `ci.ec.mdm.events.v1-value` | `ci.ec.mdm.v1.MdmEvent` |
| `ci.ec.person.events.v1` | `ci.ec.person.events.v1-value` | `ci.ec.person.v1.PersonEvent` |
| `ci.ec.civil_record.events.v1` | `ci.ec.civil_record.events.v1-value` | `ci.ec.civilrecord.v1.CivilRecordEvent` |
| `ci.ec.mention.events.v1` | `ci.ec.mention.events.v1-value` | `ci.ec.mention.v1.MentionEvent` |
| `ci.ec.mention_set.events.v1` | `ci.ec.mention_set.events.v1-value` | `ci.ec.mention.v1.MentionSetEvent` |
| `ci.ec.document.events.v1` | `ci.ec.document.events.v1-value` | `ci.ec.document.v1.DocumentEvent` |
| `ci.ec.request.events.v1` | `ci.ec.request.events.v1-value` | `ci.ec.request.v1.RequestEvent` |
| `ci.ec.extract_issuance.events.v1` | `ci.ec.extract_issuance.events.v1-value` | `ci.ec.extract.v1.ExtractIssuanceEvent` |
| `ci.ec.registry.events.v1` | `ci.ec.registry.events.v1-value` | `ci.ec.registry.v1.RegistryEvent` |
| `ci.ec.annual_table.events.v1` | `ci.ec.annual_table.events.v1-value` | `ci.ec.registry.v1.AnnualTableEvent` |
| `ci.ec.consular_record.events.v1` | `ci.ec.consular_record.events.v1-value` | `ci.ec.consular.v1.ConsularRecordEvent` |
| `ci.ec.central_record.events.v1` | `ci.ec.central_record.events.v1-value` | `ci.ec.central.v1.CentralRecordEvent` |
| `ci.ec.foreign_act.events.v1` | `ci.ec.foreign_act.events.v1-value` | `ci.ec.foreign.v1.ForeignActEvent` |
| `ci.ec.payment.events.v1` | `ci.ec.payment.events.v1-value` | `ci.ec.payment.v1.PaymentEvent` |
| `ci.ec.access.events.v1` | `ci.ec.access.events.v1-value` | `ci.ec.access.v1.AccessEvent` |
| `ci.ec.audit.events.v1` | `ci.ec.audit.events.v1-value` | `ci.ec.audit.v1.AuditEvent` |

---

## 12. Conventions event_type

Format : `<namespace>.<DomainEventName>`

Exemples :
```
ci.ec.civil_record.CivilRecordCreated
ci.ec.civil_record.CivilRecordAmended
ci.ec.extract_issuance.ExtractIssued
ci.ec.extract_issuance.ExtractVerified
ci.ec.payment.PaymentSucceeded
ci.ec.mdm.MdmVersionPublished
```

---

## 13. Compatibilité Protobuf

### Règles obligatoires
1. **Ne jamais** renuméroter ou réutiliser un numéro de tag Protobuf.
2. **Ne jamais** changer le type d'un champ existant.
3. **Ne jamais** supprimer un champ (marquer `reserved` si obsolète).
4. Les **enums** : ajouter de nouvelles valeurs en fin ; ne jamais réutiliser un numéro ; garder `0 = UNSPECIFIED`.
5. Les **oneofs** : on peut ajouter de nouveaux cas ; ne pas supprimer de cas existants.

### Règles recommandées
- Réserver les numéros de tag 1–9 pour les identifiants/clés.
- Réserver les numéros 10–19 pour les données métier principales.
- Réserver les numéros 20+ pour les données secondaires/optionnelles.
- Réserver les numéros 30+ pour les timestamps.
