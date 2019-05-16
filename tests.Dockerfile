FROM blurrcat/alpine-python-psycopg2:1.0

RUN pip install pytest

WORKDIR /app

COPY ./tests/ /app/

RUN pip install -r tests/requirements.txt

# Uncomment the next line instead of the one at the bottom (production entrypoint)
# when troubleshooting. This mkfifo stuff will end up creating a pipe, then cat consumes it, causing a wait loop
# ENTRYPOINT ["/bin/bash", "-c", "rm -f /tmp/pausepipe && mkfifo /tmp/pausepipe && cat /tmp/pausepipe"]
ENTRYPOINT ["python"]
CMD [ "-m", "pytest" ]