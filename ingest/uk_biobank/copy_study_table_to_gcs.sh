#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#

version=190430
gsutil cp configs/ukb_outputs/ukb_phenotypes.tsv gs://genetics-portal-raw/uk_biobank_sumstats/phenotypes/study_manifest.$version.tsv
