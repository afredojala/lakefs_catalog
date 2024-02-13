set dotenv-load

USER_INFO := "$(id -u):$(id -g)"

generate_lakefs_api:
  docker run \
    --rm -v ${PWD}/api_spec/:/local \
    --user {{USER_INFO}} \
    openapitools/openapi-generator-cli generate \
    -i /local/swagger.yml \
    -g python \
    -o /local/out/python \
    -t /local/templates \
    --package-name lakefs_sdk \
    -c /local/python-codegen-config.yaml

start-lakefs:
  docker start lakefs

setup-dotenv:
  cp .env.example .env

setup: generate_lakefs_api setup-dotenv
  poetry install


run-test:
  poetry run python test_lakefs.py
