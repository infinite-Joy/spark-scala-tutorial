docker run -it --rm \
  -p 8888:8888 -p 4040:4040 \
  --cpus=6.0 --memory=6000M \
  -v "$PWD/data":/home/joydeep/data \
  -v "$PWD/notebooks":/home/joydeep/notebooks "$@" \
  "$@" \
  jupyter/all-spark-notebook
