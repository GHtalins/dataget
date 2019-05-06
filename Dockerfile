FROM daocloud.io/python:3-onbuild

MAINTAINER Robin<robin.chen@b-uxin.com>

ENV LANG C.UTF-8

#设置时区,与主机保持一致

RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone


RUN  mkdir -p /trade

WORKDIR /trade

COPY ./  /trade
COPY base.txt /app
COPY requirements.txt /app

ENV PYTHONPATH "${PYTONPATH}:/trade"
ENV PYTHONPATH "${PYTONPATH}:/trade/app"

#更新pip 版本
RUN pip install -U pip

#安装Python程序运行的依赖库
RUN cd /trade && pip install -r base.txt
RUN cd /trade && pip install -r requirements.txt


EXPOSE 80


ENTRYPOINT ["python", "/trade/app/feixiaohao.py"]