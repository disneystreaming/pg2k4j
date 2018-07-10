FROM maven:latest

RUN apt-get update
RUN apt-get install -y bash curl

# Copy the entire project into the image
COPY . /src

# This will prefix all runable commands
ENTRYPOINT ["mvn"]
CMD ["clean"]

# A few examples of what to do :
# docker build . -t models
# docker run -w /src models test
# docker run -w /src models deploy