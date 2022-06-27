# Docker instructions

## Usage (vpf-class)

You can use the provided docker images to use `vpf-class` without any manual
configuration. This will automatically download the required supplementary data
at each start, so we recommend first creating a volume to persist it:
```sh
docker volume create vpf-data
```

Then to classify sequences from `seqs/test.fna` you should run

```sh
docker run --rm -it
    -v "vpf-data:/opt/vpf-tools/vpf-data" \
    -v "$PWD/seqs:/opt/vpf-tools/input-sequences:ro" \
    -v "$PWD/outputs:/opt/vpf-tools/outputs:rw" \
    bielr/vpf-tools \
        vpf-class -i input-sequences/test.fna -o outputs/test-classified
```

This command:

- Mounts the `vpf-data` volume as `/opt/vpf-tools/vpf-data`.
- Binds `./seqs` to `/opt/vpf-tools/input-sequences`, _read-only_.
- Binds `./outputs/` to `/opt/vpf-tools/outputs`.
- Automatically downloads all the required data into the `vpf-data`
Docker volume (mounted as `/opt/vpf-tools/vpf-data`) and sets
`VPF_CLASS_DATA_INDEX` to `/opt/vpf-tools/vpf-data/index.yaml`.
- Runs `vpf-class`, writing the output into the `outputs/` directory.

A free additional benefit of using it in a container is the ability to [limit
resources](https://docs.docker.com/config/containers/resource_constraints/), like
memory usage, disk access and both CPU shares and number of cores.

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
`VPF_DATA_AUTOUPDATE=0`. Note that you may need to use either `--data-index` or
set the correct value of `VPF_CLASS_DATA_INDEX` if the path to `index.yaml`
differs from the default.
