FROM public.ecr.aws/lambda/python:3.9

ARG PIP_EXTRA_INDEX_URL
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}

ARG ENVIRONMENT=${ENVIRONMENT}
ENV ENVIRONMENT=$ENVIRONMENT
ENV DD_LAMBDA_HANDLER = "etl.house_pricing.entrypoint.lambda_handler.handle"

COPY . ${LAMBDA_TASK_ROOT}

RUN  pip3 install -r ${LAMBDA_TASK_ROOT}/requirements.txt

CMD ["datadog_lambda.handler.handler"]