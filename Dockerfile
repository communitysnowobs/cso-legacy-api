FROM continuumio/miniconda3
# Use bash instead of sh
SHELL ["/bin/bash", "-c"]
# Install sudo and git
RUN apt-get update && apt-get -y install sudo git-core tmux
# Copy env file
ADD environment.yml environment.yml
# Install conda packages
RUN conda env update --prune -n base --file environment.yml
# Add source
ADD src src
WORKDIR src

#VOLUME /src/store
EXPOSE 5000
CMD ["python", "application.py"]
