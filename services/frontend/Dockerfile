# Frontend Dockerfile
# Use nginx to serve static files; this placeholder can be replaced by actual build output
FROM nginx:alpine
WORKDIR /usr/share/nginx/html
COPY . ./
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
