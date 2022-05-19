# Docker instructions

## Usage (vpf-class)

You can use the provided docker images to use `vpf-class` without any manual
configuration. For instance, to classify sequences from `seqs/test.fna`, you
should run

```sh
docker run --rm -it
    -v "$PWD/vpf-data:/opt/vpf-tools/vpf-data" \
    -v "$PWD/seqs:/opt/vpf-tools/input-sequences:ro" \
    -v "$PWD/outputs:/opt/vpf-tools/outputs:rw" \
    bielr/vpf-tools \
        vpf-class --data-index vpf-data/index.yaml \
            -i input-sequences/test.fna \
            -o outputs/test-classified
```

This command automatically downloads all the required data into `vpf-data` and
then runs `vpf-class`, writing the output into the `outputs/` directory.

## Environment variables

There are two settings that can be adjusted via environment variables (using
docker's `-e` flag before `bielr/vpf-tools`).

- `VPF_TOOLS_CHMOD` (default `1`): Due to how permissions
work in Docker, we set the container's umask to `000` and `chmod 777`
supplementary data to be readable and writable by all users (the container runs
as root). If you are using your own image with the correct UID and/or running
Docker in rootless mode, you can turn off this behaviour using `VPF_TOOLS_CHMOD=0`.
- `VPF_DATA_AUTOUPDATE` (default `1`): At each start, the container will check
if the supplementary data is both present and up-to-date. If you already have
downloaded the data yourself or simply want to disable this feature, use
`VPF_DATA_AUTOUPDATE=0`.
