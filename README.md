# vpf-tools

## The tools

Currently, only `vpf-class` is implemented.

### vpf-class

`vpf-class` attemps to classify viruses using Viral Protein Families.

Usage example 1: With existing `.hmmout` file containing the result of
performing `hmmsearch` on a FASTA file of viral proteins against the VPF models.
This simply loads the `tblout` file from `hmmsearch` and matches the best hits
against the classification of the VPFs (given in `vpf-classes.tsv`).
```sh
stack exec -- vpf-class -c vpf-classes.tsv -h hits.hmmout -o classification.tsv
```

Usage example 2: Given a `.fna` file, obtain the proteins of each virus with
`prodigal`, then perform a `hmmsearch` against the given `hmms` file. Finally
match the results against the classification of the VPFs (in `vpf-classes.tsv`).
```sh
stack exec -- vpf-class -c vpf-classes.tsv -v final_test.hmms -i test.fna -o classification.tsv
```

In this mode, concurrency options can be specified with `--workers` (number of
workers running `prodigal` and/or `hmmsearch`) and `--chunk-size` (max number
of genomes for each `prodigal`/`hmmsearch` process)

If `-o` is not specified, the result is printed to the standard output.

## Installation

Since there are still no release binaries available, you will need to install
[stack](haskellstack.org) and compile `vpf-tools` yourself. The instructions
should be the same for Mac OS and Linux.

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
