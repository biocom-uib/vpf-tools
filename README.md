# vpf-tools

## The tools

Currently, only `vpf-class` is implemented, but we have plans to include more tools
in this framework.

### vpf-class

`vpf-class` attemps to classify viruses using Viral Protein Families.

Usage example: Given a `.fna` file, obtain the proteins of each virus with
`prodigal`, then perform a `hmmsearch` against the given `hmms` (VPFs) file to
obtain a classification. This requires a working installation of
[HMMER](http://hmmer.org/) (version 3.2+) and
[Prodigal](https://github.com/hyattpd/Prodigal) (version 2.6.X). Both should be
either available in your `$PATH` or specified using the `--hmmer-prefix` and
the `--prodigal` flags.

```sh
stack exec -- vpf-class --data-index ../data/index.yaml -i ../data/test.fna -o test-classified
```

This will output a directory with a `.tsv` file for each specified classification
level in the `index.yaml` file. Using the provided files, one thus obtains:

- `test-classified/baltimore.tsv`
- `test-classified/family.tsv`
- `test-classified/genus.tsv`
- `test-classified/host_domain.tsv`
- `test-classified/host_family.tsv`
- `test-classified/host_genus.tsv`

Please read to the end to find out where to obtain all the required data files.
The `--data-files` option may be skipped if the `VPF_CLASS_DATA_INDEX`
environment variable is set to the path of `index.yaml`.

Concurrency options can be specified with `--workers` (number of
parallel workers running `prodigal` or `hmmsearch`) and `--chunk-size` (max
number of genomes for each `prodigal`/`hmmsearch` process).

## Installation

### Method 1: Docker

We now provide a pre-configured docker image which contains all the required
dependencies and automatically downloads supplementary data. See the detailed
instructions [here](DOCKER.md).

### Method 2: Building from source

Since there are no release binaries available, you will need to install
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

There is experimental support for OpenMPI. Add `--flag vpf-class:+mpi` when
building and then run the tool normally as any other program with `mpirun`.

## Supplementary material

You can find our classification of VPFs either as a
compressed package (including `index.yaml`)
[here](https://bioinfo.uib.es/~recerca/VPF-Class/vpf-class-data.tar.gz).

Alternatively, you can download individual data files
[here](https://bioinfo.uib.es/~recerca/VPF-Class/), at the "VPF classification"
tab. The data files that `vpf-class` requires are in the rows "Full data"
(`modelClassesFile`) and "UViG Score samples" (`scoreSamplesFile`).  This VPF
classification has been obtained as described in the paper, but the tool is
designed to work with any user-provided classification files.

The most recent `hmms` file containing the HMMER models of VPFs (`vpfsFile` in
`data-index.yml`) can be downloaded from
[IMG/VR](https://img.jgi.doe.gov//docs/final_list.hmms.gz) (UPDATE: the link
appears to be broken, you can find a copy
[here](https://bioinfo.uib.es/~recerca/VPF-Class/vpf-class-data/final_list.hmms)).
To use it with the provided `index.yaml`, extract `final_list.hmms` into the
`data` directory, next to `index.yaml`.

## Troubleshooting

- **HMMSearchNotFound**: First, make sure that you have a working installation
of [HMMER](http://hmmer.org/). If it is not accessible from your `$PATH`, you
can specify the path to the installation (the directory that contains `bin` and
`share`) using the `--hmmer-prefix` flag.

- **ProdigalNotFound**: Make sure that you have
[Prodigal](https://github.com/hyattpd/Prodigal) installed. If it is not
accessible from your `$PATH`, you can specify the location to the executable
using the `--prodigal` flag.

- **The first step (`curl -sSL https://get.haskellstack.org/ | sh`) requires
root access**: The default configuration in the Stack installer uses
`/usr/local/` as the default prefix. Stack can also be installed in
`$HOME/.local/` following their manual installation method.

- **Stack build reports errors either while installing GHC or downloading package
indices**: If you have any issues during the installation, please check out the [Stack
documentation](https://docs.haskellstack.org/en/stable/install_and_upgrade/) to
verify that all dependencies are satisfied.

- **I have issues with conda**: Some users have reported issues with Stack and
Conda. Thus, installing it in a Conda-polluted environment is discouraged and
unsupported.
