# vpf-tools

## The tools

Currently, only `vpf-class` is implemented, but we have plans to include more tools
in this framework.

### vpf-class

`vpf-class` attemps to classify viruses using Viral Protein Families.

Usage example: Given a `.fna` file, obtain the proteins of each virus with
`prodigal`, then perform a `hmmsearch` against the given `hmms` (VPFs) file to
obtain a classification.
```sh
stack exec -- vpf-class --data-index ../data/data-index.yaml -i ../data/test.fna -o test-classified
```

This will output a directory with a `.tsv` file for each given classification
type. For instance, if the contents of `../data/data-index.yaml` are

```yaml
classificationFiles:
  baltimore:
    modelClassesFile: ./vpf-classification/baltimore.tsv
    scoreSamplesFile: ./score-samples/baltimore.tsv
  family:
    modelClassesFile: ./vpf-classification/family.tsv
    scoreSamplesFile: ./score-samples/family.tsv
  genus:
    modelClassesFile: ./vpf-classification/genus.tsv
    scoreSamplesFile: ./score-samples/genus.tsv
  host_domain:
    modelClassesFile: ./vpf-classification/domain.tsv
    scoreSamplesFile: ./score-samples/host_domain.tsv
  host_family:
    modelClassesFile: ./vpf-classification/family.tsv
    scoreSamplesFile: ./score-samples/host_family.tsv
  host_genus:
    modelClassesFile: ./vpf-classification/genus.tsv
    scoreSamplesFile: ./score-samples/host_genus.tsv

vpfsFile: final_list.hmms
```

then `vpf-class` produces the following files:

- `test-classified/baltimore.tsv`
- `test-classified/family.tsv`
- `test-classified/genus.tsv`
- `test-classified/host_domain.tsv`
- `test-classified/host_family.tsv`
- `test-classified/host_genus.tsv`


Concurrency options can be specified with `--workers` (number of
workers running `prodigal` or `hmmsearch`) and `--chunk-size` (max number
of genomes for each `prodigal`/`hmmsearch` process).

## Installation

Since there are still no release binaries available, you will need to install
[stack](haskellstack.org) and compile `vpf-tools` yourself. The instructions
are the same for both Mac OS and Linux, the tool has not been tested on
Windows.

First, install stack using
```sh
curl -sSL https://get.haskellstack.org/ | sh
```

Then run
```sh
git clone https://github.com/biocom-uib/vpf-tools
cd vpf-tools
stack build
```
to clone the repository and compile all targets. The first time this can take a
while as `stack` also needs to install GHC and compile all the dependencies.
Once it has finished, you should be able to run any of the tools from this
directory by prefixing them with `stack exec --`, for instance,
```sh
stack exec -- vpf-class --help
```

There is experimental support for MPI. Add `--flag vpf-class:+mpi` when
building and then run the tool normally using `mpirun`.

## Supplementary material

You can find our classification of VPFs at [http://bioinfo.uib.es/~recerca/VPF-Class/]

Score samples will be added soon, and only affect confidence scores. If
you do not require them, you can use placeholder text files containing any single
number.

The `hmms` file containing the actual VPFs can be downloaded from (IMG/VR)[https://img.jgi.doe.gov/virus/doc/final_list.hmms.gz]
