FROM openjdk:8-jdk-alpine

######
RUN apk update && apk upgrade

ENV BASEDIR /opt/ratis-logservice/

# Our ratis-logservice tarball
ARG BINARY
ARG VERSION

# Extract, symlink, and remove the ratis-logservice tarball
COPY $BINARY $BASEDIR
RUN tar xf ${BASEDIR}/ratis-logservice-${VERSION}-bin.tar.gz -C $BASEDIR \
    && ln -s ${BASEDIR}/ratis-logservice-${VERSION} ${BASEDIR}/current \
    && rm ${BASEDIR}/ratis-logservice-${VERSION}-bin.tar.gz

# Use the nice symlink we made as our "starting directory"
WORKDIR $BASEDIR/current
