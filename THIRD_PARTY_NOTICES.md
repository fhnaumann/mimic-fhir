# Third-party notices

This file lists third-party material that is included in this repository, where it is, who
holds the rights to it, and the terms it is used under.

This repository is a fork of [kind-lab/mimic-fhir](https://github.com/kind-lab/mimic-fhir); its `LICENSE` is the upstream MIT licence, Copyright (c) 2023 Alistair Johnson, Alex Bennett, Evan Ng, and covers the upstream code.

Packages installed by a package manager (PyPI, npm) are not vendored here and are not listed;
their licences ship with the packages.

## Upstream: kind-lab/mimic-fhir

Everything not listed under *Fork additions* below is upstream content by the mimic-fhir authors
(KinD Lab and contributors), MIT-licensed as stated in [`LICENSE`](LICENSE).

**Fork additions and changes** by the fork's owner (from 2026-08): `duckdb/`, `metrics/`,
`hpc/`, the `debug-*/` directories, `BENCHMARK.md`, `DUCKDB_PORT_PLAN.md`, `HANDOFF.md`,
`PORT_NOTES_DUCKDB_SPARK.md`, `PSQL_*.md`, and fixes to four files under `sql/`
(`git log` has the detail).

## Submodules

`mimic-code` (MIT-LCP/mimic-code, MIT), `mimic-profiles` (kind-lab/mimic-profiles) and
`fhir-packages` (kind-lab/fhir-packages) are git submodules: only their commit pointers are
stored here, not their content.

## HAPI FHIR JPA Server Starter

`hapi-fhir-jpaserver-starter/pom.xml` comes from the HAPI FHIR JPA Server Starter project
(<https://github.com/hapifhir/hapi-fhir-jpaserver-starter>, University Health Network / Smile
CDR), Apache License 2.0, <https://www.apache.org/licenses/LICENSE-2.0>. It was added upstream.

## Clinical terminologies

The FHIR conversion SQL (`sql/`, upstream) assigns a few standard codes with display names:
about 19 LOINC codes (vital signs, ED observations), a handful of SNOMED CT codes (encounter and
procedure types) and UCUM unit codes, besides HL7 FHIR/THO codes and the MIMIC-on-FHIR code
systems.

### SNOMED CT

> This material includes SNOMED Clinical Terms® (SNOMED CT®) which is used by permission of
> SNOMED International. All rights reserved. SNOMED CT® was originally created by The College of
> American Pathologists. "SNOMED" and "SNOMED CT" are registered trademarks of SNOMED
> International.

Use of SNOMED CT content requires the user to hold an appropriate licence: a SNOMED International
Affiliate Licence, or a national release licence in a SNOMED International Member country. See
<https://www.snomed.org/get-snomed>.

### LOINC

> This material contains content from LOINC® (http://loinc.org). LOINC is copyright © Regenstrief
> Institute, Inc. and the Logical Observation Identifiers Names and Codes (LOINC) Committee and is
> available at no cost under the license at http://loinc.org/license. LOINC® is a registered
> United States trademark of Regenstrief Institute, Inc.

### UCUM

> This product includes all or a portion of the UCUM table, UCUM codes, and UCUM definitions or is
> derived from it, subject to a license from Regenstrief Institute, Inc. and The UCUM
> Organization. Your use of the UCUM table, UCUM codes, UCUM definitions also is subject to this
> license, a copy of which is available at http://unitsofmeasure.org. The current complete UCUM
> table, UCUM Specification are available for download at http://unitsofmeasure.org. The UCUM
> table and UCUM codes are copyright © 1995-2013, Regenstrief Institute, Inc. and the Unified
> Codes for Units of Measures (UCUM) Organization. All rights reserved.

### HL7® FHIR® and HL7 Terminology (THO)

FHIR resource and element names, and codes from FHIR and HL7 Terminology (`http://hl7.org/fhir/...`,
`http://terminology.hl7.org/...`) code systems, come from the FHIR specification and HL7
Terminology, which HL7 publishes under the Creative Commons CC0 public-domain dedication
(<https://www.hl7.org/fhir/license.html>). No notice is required. HL7® and FHIR® are registered
trademarks of Health Level Seven International; their use does not constitute endorsement by HL7.

## MIMIC-IV data

The conversion code itself contains no MIMIC-IV data.

### MIMIC-IV demo datasets (Open Database License)

The open MIMIC-IV demo releases on PhysioNet are licensed under the **Open Data Commons Open
Database License v1.0 (ODbL)**, <https://opendatacommons.org/licenses/odbl/1-0/>:

- **MIMIC-IV Clinical Database Demo**, version 2.2. Johnson A, Bulgarelli L, Pollard T, Horng S,
  Celi LA, Mark R. PhysioNet (2023). <https://doi.org/10.13026/dp1f-ex47>
- **MIMIC-IV-ED Demo**, version 2.2. Johnson A, Bulgarelli L, Pollard T, Celi LA, Horng S, Mark R.
  PhysioNet (2023). <https://doi.org/10.13026/jzz5-vs76>
- **MIMIC-IV Clinical Database Demo on FHIR**. Bennett A, Ulrich H, Wiedekopf J, Szul P, Grimes J,
  Johnson A. PhysioNet. <https://physionet.org/content/mimic-iv-fhir-demo/>

PhysioNet: Goldberger A, Amaral L, Glass L, Hausdorff J, Ivanov PC, Mark R, Mietus JE, Moody GB,
Peng CK, Stanley HE. PhysioBank, PhysioToolkit, and PhysioNet: Components of a new research
resource for complex physiologic signals. *Circulation* 101(23):e215–e220 (2000). PhysioNet's
current standard citation is Pollard T, Moody BE, Lehman L, et al. PhysioNet as a global platform
for biomedical research. *Nature Health* (2026). <https://doi.org/10.1038/s44360-026-00096-z>

> Contains information from the MIMIC-IV Clinical Database Demo, which is made available here
> under the Open Database License (ODbL).

The demo-derived files listed below are derived databases of these releases. Under ODbL §4.4 a
derived database that is publicly used must itself be offered under the ODbL, so these data files
are subject to the ODbL v1.0. This applies to those data files only, not to the code in this
repository.

Demo-derived files in this repository:

- `debug-patient-birthdate-anchor/hpc/results/demo-smoke/*` (`affected_patients.csv`,
  `anchor_year_identities.csv`, `error_distribution.csv`, `examples.csv`, `report.txt`,
  `summary.json`): output of the birth-date diagnostic run on the demo.

### MIMIC-IV (credentialed)

MIMIC-IV (Johnson A, Bulgarelli L, Pollard T, Horng S, Celi LA, Mark R. MIMIC-IV, version 2.2.
PhysioNet (2023), <https://doi.org/10.13026/6mm1-ek67>; Johnson AEW, Bulgarelli L, Shen L, et al.
MIMIC-IV, a freely accessible electronic health record dataset. *Sci Data* 10, 1 (2023)) is
available only to credentialed users under the **PhysioNet Credentialed Health Data License 1.5.0**
and the PhysioNet Credentialed Health Data Use Agreement 1.5.0. No patient-level data from it is
included in this repository.

Full-dataset runs appear here only as aggregate build and size metrics (`metrics/`) and
aggregate figures in the notes.
