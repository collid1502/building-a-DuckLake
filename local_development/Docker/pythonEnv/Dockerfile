# Use a Miniconda image as base
FROM continuumio/miniconda3

# Always use bash in RUN/CMD
SHELL ["/bin/bash", "-c"]

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace

# Install common OS packages
RUN apt-get update && \
    apt-get install -y curl git zip cron && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy in the environment.yml file and build the Conda environment
COPY environment.yml environment.yml
RUN conda env create -f environment.yml

# Set a volume for workspace
VOLUME /workspace

# Create a non-root user
ARG USERNAME=collid
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

RUN groupadd --gid ${USER_GID} ${USERNAME} && \
    useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} && \
    chown -R ${USERNAME}:${USERNAME} /workspace

# Initialize conda for bash & auto-activate 'duck_etl' for the non-root user
RUN conda init bash

USER ${USERNAME}

# Activate Conda env in bash sessions
RUN echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate duck_etl" >> ~/.bashrc && \
    echo ". /opt/conda/etc/profile.d/conda.sh && conda activate duck_etl" >> ~/.profile

# Copy your cron setup script into the image
COPY --chown=collid:collid setup_cron.sh /home/collid/setup_cron.sh
RUN chmod +x /home/collid/setup_cron.sh

# Disable auto-activation of base Conda env globally
USER root
RUN conda config --system --set auto_activate_base false

# Switch back to non-root user
USER ${USERNAME}

# Default command to keep the container running
CMD ["bash", "-c", "sleep infinity"]