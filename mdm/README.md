# MDM CI-EC — Documentation

## Qu'est-ce que le MDM CI-EC ?

Le **Master Data Management (MDM) CI-EC** gère tous les référentiels de codes utilisés dans le système d'état civil ivoirien.

---

## Règle MDMV1 (normative)

Toute référence MDM dans un **snapshot d'événement Kafka** DOIT contenir :

```json
{
  "system":  "CI-EC-MDM",
  "code":    "SEX.MALE",
  "version": "2026-04"
}
```

- `system` = toujours `"CI-EC-MDM"`
- `code` = code canonique du référentiel
- `version` = version MDM **obligatoire** (ex: `"2026-04"`) — le write-service injecte la version active à la date de l'événement

> **Pourquoi MDMV1 ?** Audit-proof + reproductibilité : chaque événement sait exactement avec quelle version MDM il a été produit. Le read-side n'a jamais à "deviner" la version.

---

## Structure d'un fichier MDM

```yaml
system:     "CI-EC-MDM"
codeset:    "PLACE"          # nom du référentiel
version:    "2026-04"        # version de publication
status:     PUBLISHED
published_at: "2026-04-01T00:00:00Z"

values:
  - code: "CI-AB-001"
    label: "Abidjan – Plateau"
    labels:
      fr: "Abidjan – Plateau"
      en: "Abidjan – Plateau District"
    active: true
    valid_from: "2026-04-01"
    attributes: {}
```

---

## Référentiels disponibles (codesets)

| Codeset             | Exemple de code              | Description                                |
|---------------------|------------------------------|--------------------------------------------|
| `PLACE`             | `CI-AB-001`                  | Codes lieux état civil (P1)                |
| `SEX`               | `SEX.MALE`, `SEX.FEMALE`     | Genre                                      |
| `CIVIL_RECORD.TYPE` | `CIVIL_RECORD.TYPE.BIRTH`    | Type d'acte civil                          |
| `CIVIL_RECORD.STATUS` | `CIVIL_RECORD.STATUS.ACTIVE` | Statut acte                              |
| `MENTION.TYPE`      | `MENTION.TYPE.MARRIAGE`      | Type de mention marginale                  |
| `MENTION.STATUS`    | `MENTION.STATUS.ACTIVE`      | Statut mention                             |
| `REQUEST.TYPE`      | `REQUEST.TYPE.EXTRACT_ISSUANCE` | Type de demande                         |
| `REQUEST.STATUS`    | `REQUEST.STATUS.SUBMITTED`   | Statut demande                             |
| `REQUEST.CHANNEL`   | `REQUEST.CHANNEL.WEB`        | Canal de soumission                        |
| `EXTRACT.KIND`      | `EXTRACT.KIND.E2`            | Type d'extrait (E2/V1/M2/H2)              |
| `EXTRACT.STATUS`    | `EXTRACT.STATUS.ISSUED`      | Statut extrait                             |
| `RENDER.MODE`       | `RENDER.MODE.DIGITAL`        | Mode de rendu (PAPER/DIGITAL)             |
| `PAYMENT.METHOD`    | `PAYMENT.METHOD.MOBILE_MONEY` | Méthode de paiement                      |
| `PAYMENT.STATUS`    | `PAYMENT.STATUS.COMPLETED`   | Statut paiement                            |
| `REGISTRY.TYPE`     | `REGISTRY.TYPE.BIRTH`        | Type de registre                           |
| `FOREIGN.ACT.TYPE`  | `FOREIGN.ACT.TYPE.BIRTH`     | Type d'acte étranger                       |
| `FOREIGN.ACT.STATUS` | `FOREIGN.ACT.STATUS.RECEIVED` | Statut acte étranger                    |
| `ACCESS.BASIS`      | `ACCESS.BASIS.LEGAL_DUTY`    | Base légale d'accès                        |
| `ACCESS.DENY_REASON` | `ACCESS.DENY_REASON.NO_RIGHT` | Motif de refus d'accès                   |
| `AUDIT.ACTION`      | `AUDIT.ACTION.READ`          | Action auditée                             |
| `NATIONALITY`       | `NATIONALITY.CI`             | Nationalité (ISO-3166-1 alpha-2)           |
| `CIVIL_STATUS`      | `CIVIL_STATUS.SINGLE`        | Situation matrimoniale                     |
| `DOCUMENT.TYPE`     | `DOCUMENT.TYPE.ID_CARD`      | Type de pièce justificative               |
| `DOCUMENT.STATUS`   | `DOCUMENT.STATUS.VALIDATED`  | Statut document                            |

---

## Gestion des versions

- Les versions suivent le format `YYYY-MM` (ex: `2026-04`).
- Statuts : `DRAFT` → `PUBLISHED` → `DEPRECATED`
- Compatibilité BACKWARD : les nouvelles versions ajoutent des codes, ne suppriment pas.
- Chaque publication déclenche un événement `MdmVersionPublished` sur le topic `ci.ec.mdm.events.v1`.

---

## Fichiers disponibles

| Fichier | Contenu |
|---------|---------|
| `ci-ec-mdm-place-2026-04.yaml` | Codes lieux (PLACE P1) — version 2026-04 |

---

## Schema Registry (SR2 — Confluent)

- Subject : `ci.ec.mdm.events.v1-value`
- Message racine : `ci.ec.mdm.v1.MdmEvent`
- Compatibilité : `BACKWARD`
- Strategy : `TopicNameStrategy`
