import os
import subprocess
import yaml
from dotenv import dotenv_values
import argparse
import time
today = time.strftime("%Y%m%d", time.localtime())
script_path = os.path.dirname(__file__)
parser = argparse.ArgumentParser( description='Mito Calling Pipeline')
parser.add_argument( 'infile', type=str, help='input csv, header: sample,r1,r2')
parser.add_argument( 'project', type=str, help='project name')
parser.add_argument( '--outdir', type=str, help='output directory',
                    default = f"mito-calling-{today}")
parser.add_argument( '--cores', type=int, help='cpu cores', default = 50)
parser.add_argument( '--env', type=str, help='env files', default = f"{script_path}/.env")
args = parser.parse_args()
os.makedirs(args.outdir, exist_ok=True)
# read command line parameters
invars = {"infile": os.path.abspath(args.infile),
          "outdir": os.path.abspath(args.outdir),
          "project": args.project}

# read env parameters
envdict = dotenv_values(args.env)

# write all parameters into snakemake input yaml
snake_config = os.path.join(os.path.abspath(args.outdir), "mito-calling.input.yaml")
stream = open(snake_config, 'w')
yaml.dump(invars | envdict, stream)
stream.close()

# run snakemake
cmd = f"snakemake -s {script_path}/workflow/Snakefile --configfile {snake_config} -c {args.cores}"
print(cmd)
subprocess.run(cmd, shell=True)
