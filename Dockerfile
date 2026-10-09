FROM python:3.13-slim AS build
WORKDIR /src
COPY docs/requirements.txt docs/requirements.txt
RUN pip install --no-cache-dir -r docs/requirements.txt
COPY . .
RUN mkdocs build

FROM nginx:1.27-alpine
COPY --from=build /src/site /usr/share/nginx/html
RUN printf 'server {\n  listen 8080;\n  root /usr/share/nginx/html;\n  location / { try_files $uri $uri/ $uri.html =404; }\n  error_page 404 /404.html;\n}\n' > /etc/nginx/conf.d/default.conf
EXPOSE 8080
