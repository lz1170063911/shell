#!/usr/bin/env python
# -*- coding:utf-8 -*-

import requests
import pymysql
from requests.auth import HTTPBasicAuth


# 初始化连接
def handle_urlopen(url, auth_user="admin", auth_password="jcloud-es", *args):
    headers = {"Content-Type": "application/json"}
    password_mgr = HTTPBasicAuth(auth_user, auth_password)
    response = requests.get(url=url, headers=headers, auth=password_mgr, timeout=(3, 5))
    return response


# 获取集群健康状态
def fetch_clusterhealth(elasticServer):
    try:
        url = 'http://' + elasticServer.get('domain') + ':' + str(elasticServer.get('port'))
        endpoint = "/_cluster/health"
        urlData = url + endpoint
        tags_cluterName = elasticServer.get('domain').split(".")[0]

        response = handle_urlopen(url=urlData)
        jsonData = response.json()

        unassigned_shards = jsonData['unassigned_shards']

        if jsonData['status'] == 'green':
            print('tags:clustername:%s' % tags_cluterName)
            print('status:0,desc:当前集群状态为green且未分配分片数为%s' % unassigned_shards)
        elif jsonData['status'] == 'yellow':
            print('tags:clustername:%s' % tags_cluterName)
            print('status:1,desc:当前集群状态为yellow且未分配分片数为%s' % unassigned_shards)
        elif jsonData['status'] == 'red':
            print('tags:clustername:%s' % tags_cluterName)
            print('status:2,desc:当前集群状态为red且未分配分片数为%s' % unassigned_shards)
        else:
            print('tags:clustername:%s' % tags_cluterName)
            print('status:-1,desc:集群访问超时')

    except Exception as e:
        print('tags:clustername:%s' % tags_cluterName)
        print('status:-1,desc:集群访问超时')


# 主函数
def main():
    try:
        connection = pymysql.connect(host="middlewareop.mysql-prod-bj01.jdcloud.com", port=3306, user="root", password="hodor", database="hodor",
                                     charset='utf8')
        cursor = connection.cursor(cursor=pymysql.cursors.DictCursor)
        sql = "SELECT domain,port from es_cluster"
        cursor.execute(sql)
        es_cluster_list = cursor.fetchall()
        if es_cluster_list:
            print('tags:clusterHealthMonitor:connection-mysql')
            print('status:0,desc:mysql数据库连接正常')
            for item in es_cluster_list:
                fetch_clusterhealth(elasticServer=item)
        else:
            print('tags:clusterHealthMonitor:connection-mysql-fail')
            print('status:1,desc:mysql数据库连接异常')
    except Exception as e:
        print('tags:clusterHealthMonitor:connection-mysql')
        print('status:1,desc:mysql数据库连接异常')


if __name__ == '__main__':
    main()
