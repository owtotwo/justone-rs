# justone -- A fast duplicate files command line finder

`author: owtotwo`


## Install

```
$ cargo install justone
```


## Usage

```
justone 0.2.0
owtotwo <owtotwo@163.com>
A fast duplicate files finder, the rust implementation for JustOne.

USAGE:
    justone [FLAGS] [OPTIONS] <FOLDER>...

FLAGS:
    -h, --help
            Prints help information

    -i, --ignore-error
            Ignore error such as PermissionError or FileNotExisted

    -s, --strict
            [0][default] Based on hash comparison.
            [1][-s] Shallow comparison based on file stat, and byte comparison when inconsistent, to prevent hash
            collision.
            [2][-ss] Strictly compare byte by byte to prevent file stat and hash collision.
    -t, --time
            Show total time consumption

    -V, --version
            Prints version information


OPTIONS:
    -o, --output <output>
            Output result to file


ARGS:
    <FOLDER>...
            The folder where you want to find duplicate files

```


## Uninstall

```
$ cargo uninstall justone
```


## License
[LGPLv3](./License) © [owtotwo](https://github.com/owtotwo)
