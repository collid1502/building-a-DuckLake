# Use a Miniconda image as base
FROM continuumio/miniconda3

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace

RUN apt-get update && \
    apt-get install -y curl git zip

# copy in the environment.yml file
COPY environment.yml environment.yml

# Build conda environment based off YML file above
RUN conda env create -f environment.yml

# set volume for workspace
VOLUME /workspace

# Set a user to avoid being ROOT
ARG USERNAME=collid
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

RUN groupadd --gid ${USER_GID} ${USERNAME} && \
    useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} && \
    chown -R ${USERNAME}:${USERNAME} /workspace

# Conda init & auto-activate for non-root user
USER ${USERNAME}
RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate duck_etl" >> ~/.bashrc

RUN echo ". /opt/conda/etc/profile.d/conda.sh && conda activate duck_etl" >> ~/.profile

# Avoid auto-activating base environment globally
USER root
RUN conda config --set auto_activate_base false

USER ${USERNAME}

# Default command
CMD ["sleep", "infinity"]