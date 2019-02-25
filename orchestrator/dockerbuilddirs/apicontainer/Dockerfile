FROM nginx:alpine

ADD nginx.conf /etc/nginx/
COPY api.conf /etc/nginx/sites-available/

ARG AUTH_KUBERNETES_CLUSTER_IP_ADDRESS
ARG GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS
ARG GA_CHP_BQ_KUBERNETES_CLUSTER_IP_ADDRESS

RUN apk update \
    && apk upgrade \
    && apk add --no-cache bash \
    && adduser -D -H -u 1000 -s /bin/bash www-data \
    && rm /etc/nginx/conf.d/default.conf \
    && echo -e "upstream kubernetes-upstream-auth { server ${AUTH_KUBERNETES_CLUSTER_IP_ADDRESS}; } \n" > /etc/nginx/conf.d/upstream.conf \
    && echo -e "upstream kubernetes-upstream-ga-chp { server ${GA_CHP_KUBERNETES_CLUSTER_IP_ADDRESS}; } \n" >> /etc/nginx/conf.d/upstream.conf \
    && echo -e "upstream kubernetes-upstream-ga-chp-bq { server ${GA_CHP_BQ_KUBERNETES_CLUSTER_IP_ADDRESS}; } \n" >> /etc/nginx/conf.d/upstream.conf

CMD ["nginx"]

EXPOSE 80 443