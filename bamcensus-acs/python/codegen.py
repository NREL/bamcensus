import os
import re
import argparse

parser = argparse.ArgumentParser(
    "extracts acs variables from scraped HTML and generates code",
    description="""as of today, it appears ACS variables are only listed in HTML format
    on their website. this file expects the HTML table format and extracts group, name, and id
    for all variables. the user has to remove metavariables from the list first, which they include,
    such as "for" and "if" and the hierarchical arguments like "STATE. this script expects a file
    formatted with newlines and 2-space indents but that can be modified with the CLI arguments.""",
)
parser.add_argument("filename", help="source HTML file")
parser.add_argument("--csv", type=str, help="CSV target filename")
parser.add_argument("--rs", type=str, help="rust target filename for codegen")
parser.add_argument(
    "--indent",
    type=int,
    default=2,
    help="intent between HTML tags. if 0, newlines are also elided.",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="if provided, overwrite existing output file if exists",
)


def to_name_path(name: str):
    return name.lower().replace("!!", ".").replace(":", "").replace(" ", "_")


def write_csv(matches, csv_filename):
    csv_header = "group,variable,path"
    with open(csv_filename, "w") as f:
        f.write(csv_header + "\n")
        for identifier, name in matches:
            group_id, sgroup_id = identifier.split("_")
            name_path = to_name_path(name)
            f.write(f"{group_id},{sgroup_id},{name_path}\n")


def write_rust(matches, rust_filename):
    class_name = "ClassName"

    def to_string_gen(group, name):
        return f'                C::{group} => String::from("{name}")'

    def to_path_gen(group, name):
        path = to_name_path(name)
        return f'                C::{group} => Ok(String::from("{path}"))'

    enums = ",\n".join([f"    {g}" for g, _ in matches])
    tostr = ",\n".join([to_string_gen(g, n) for g, n in matches])
    topath = ",\n".join([to_path_gen(g, n) for g, n in matches])

    codegen = r"""use serde::{{Deserialize, Serialize}};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum {classname} {{
{enums}
}}

impl ToString for {classname} {{
    fn to_string(&self) -> String {{
        use {classname} as C;
        match self {{
{tostr}
        }}
    }}
}}

impl {classname} {{
    
    fn to_path(&self) -> String {{
        use {classname} as C;
        match self {{
{topath}
        }}
    }}
}}
    """.format(classname=class_name, enums=enums, tostr=tostr, topath=topath)
    with open(rust_filename, "w") as f:
        f.write(codegen)


def run():
    args = parser.parse_args()

    name_pat = "[\w:! ]"
    if args.indent == 0:
        pattern = rf"""<tr><td><a href="variables\/.*" name=".*">(\w+_\w+)<\/a><\/td><td>({name_pat}+)<\/td>"""
    else:
        sep = " " * args.indent
        pattern = rf"""<tr>\n{sep}<td><a href="variables\/.*" name=".*">(\w+_\w+)<\/a><\/td>\n{sep}<td>({name_pat}+)<\/td>"""

    with open(args.filename) as f:
        contents = f.read()
    matches = re.findall(pattern, contents)

    if len(matches) == 0:
        print("no matches found in source file, check formatting")
        parser.print_usage()
        return 1

    if args.csv is not None and (not os.path.exists(args.csv) or args.overwrite):
        write_csv(matches, args.csv)
    if args.rs is not None and (not os.path.exists(args.rs) or args.overwrite):
        write_rust(matches, args.rs)

    return 0


if __name__ == "__main__":
    run()
