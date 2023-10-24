RUN curl -kfLO XXX.tar.gz
RUN tar -xzf code-server-4.16.1.tar.gz
RUN curl -kO YYY/python-2023.12.0.vsix
RUN curl -kO ZZZ/Jupyter-2023.4.1001091014.vsix
RUN code-server-4.16.1-linux-amd64/bin/code-server --install-extension python-2023.12.0.vsix
RUN code-server-4.16.1-linux-amd64/bin/code-server --install-extension Jupyter-2023.4.1001091014.vsix