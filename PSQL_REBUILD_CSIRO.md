## PSQL Setup on HPC

On the HPC cluster there are modules for postgresql 15.2 and 16.0 but:
- the uuid-ossp extension is not available for 15.2
- 16.0 does not appear to be setup correctly (errors running pg_ctl)

### Making a copy of postgres 15.2 

```bash
mkdir -p $DATASETS/reference/apps/postgresql
cp -r /apps/postgres/15.2 $DATASETS/reference/apps/postgresql
```

### Installing uuid-ossp extension (for v 15.2)

Set `PROJ_DIR` to this project root directory.

```bash
git clone https://github.com/postgres/postgres.git
git checkout REL_15_2
cd postgres

${PROJ_DIR}/hpc/env/psql/uuid-ossp_buildme_15_2.sh confiure
${PROJ_DIR}hpc/env/psql/uuid-ossp_buildme_15_2.sh build
${PROJ_DIR}hpc/env/psql/uuid-ossp_buildme_15_2.sh install
```

This will build the extension and install in the posgresql copy in: `$DATASETS/reference/apps`


### Database setup

- add `$DATASETS/reference/apps/postgresql/15.2/bin` to your path

To create a new database:

```bash
initdb -A trust -D ${DB_DIR}
```

Update the `postgresql.conf` file in `${DB_DIR}` to include the following:
with the `env/psql/postgresql.conf` file.

See: https://chatgpt.com/share/f60dd22c-5450-46ad-b2a9-a76ec69a38c5 for more details on the configuration.


To start stop:
  
```bash 
pg_ctl -D ${DB_DIR} start
pg_ctl -D ${DB_DIR} stop
```


## Fhir Validator Setup on HPC

Modules:

- amazon-corretto/11.0.14.10.1
- maven/3.8.1

```bash
module load amazon-corretto/11.0.14.10.1 maven/3.8.1 
```

Then use conda env (TBP)

- conda with python 3.11
- pip install -r env/requirements.txt

Setup spark configuration defaults (TBP):

```bash
export SPARK_CONF_DIR=$PWD/env/spark-conf
```
