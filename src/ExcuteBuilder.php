<?php

declare(strict_types=1);

namespace Liguizhou\Elasticsearch;

use Hyperf\Utils\Collection;

/**
 * Class Builder
 * @package Elasticsearch
 * Date : 2023/7/17
 * Author: lgz
 * Desc: ElasticSearch 链式操作
 */
trait ExcuteBuilder
{
    //原始方法
    public function all($body, $method)
    {
        $this->sql = $body;
        $result = $this->run($method);
        return $result;
    }

    /**
     * 单条插入，存在更新
     * @param array $value
     * @param array $fields
     * @return bool
     * @throws \Exception
     */
    public function insert(array $value, array $fields = [])
    {
        $data = empty($fields) ? $value : Arr::only($value, $fields);
        $body = [
            'index' => $this->model->getIndex(),
            'type'  => '_doc',
            'body'  => $data
        ];
        if (!empty($value['id'])) {
            $body['id'] = $value['id'];
        }
        $this->sql = $body;
        $result = $this->run('index');

        if (!empty($result['result'])) {
            $this->model->setOriginal($result);
            $this->model->setAttributes(Arr::merge($body, ['_id' => $result['_id'] ?? '']));
            return $this->model->getAttributes();
        } else {
            return $result;
        }
    }

    public function batchInsert(array $values, array $fields = [])
    {
        $body = [];
        foreach ($values as $value) {
            $tmp = [
                'index' => ['_index' => $this->model->getIndex()],
            ];
            if (!empty($value['id'])) {
                $tmp['index']['_id'] = $value['id'];
            }
            $body['body'][] = $tmp;
            $data = empty($fields) ? $value : Arr::only($value, $fields);
            $body['body'][] = $data;
        }
        $this->sql = $body;
        $result = $this->run('bulk');

        $collection = collect($result['items'])->map(function ($value, $key) use ($values) {
            $model = $this->model->newInstance();
            $model->setOriginal($value);
            $model->setAttributes(Arr::merge($values[$key] ?? [], ['_id' => $value['index']['_id'] ?? '']));
            return $model;
        });

        return $collection;
    }

    public function update(array $value, $id)
    {
        $body = [
            'index' => $this->model->getIndex(),
            'type'  => '_doc',
            'id'    => $id,
            'body'  => [
                'doc' => $value
            ]
        ];

        $this->sql = $body;
        $result = $this->run('update');

        if (!empty($result['result']) && ($result['result'] == 'updated' || $result['result'] == 'noop')) {
            $this->model->setOriginal($result);
            $this->model->setAttributes(['_id' => $result['_id'] ?? '']);
            return $this->model->getAttributes();
        }
        return $result;
    }


    public function updateByQuery(array $body)
    {
        $query = $this->parseQuery();
        if (empty($query)) { //先不允许全更新
            return [];
        }
        $source = '';
        foreach ($body as $key => $value) {
            $source .= 'ctx._source.' . $key . '=params.' . $key . ';';
        }
        $body = [
            'index' => $this->model->getIndex(),
            'body'  => [
                'query'  => $query,
                'script' => [
                    'source' => $source,
                    'params' => $body
                ]
            ]
        ];

        $this->sql = $body;
        $result = $this->run('updateByQuery');

        return $result;
    }

    public function createIndex(array $values, array $settings = [])
    {
        $properties = [];
        foreach ($values as $key => $value) {
            if (is_array($value)) {
                $properties[$key] = $value;
            } else {
                $properties[$key] = ['type' => $value];
            }
        }
        $body = [
            'index' => $this->model->getIndex(),
            'body'  => [
                'settings' => [
                    'number_of_shards'   => 1,
                    'number_of_replicas' => 1
                ],
                'mappings' => [
                    '_source'    => [
                        'enabled' => true
                    ],
                    'properties' => $properties
                ]
            ]
        ];
        if (!empty($settings)) {
            $body['body']['settings'] = $settings;
        }

        $this->sql = $body;
        $result = $this->run('indices.create');

        return $result;
    }

    public function updateIndex(array $values)
    {
        $properties = [];
        foreach ($values as $key => $value) {
            if (is_array($value)) {
                $properties[$key] = $value;
            } else {
                $properties[$key] = ['type' => $value];
            }
        }

        $body = [
            'index' => $this->model->getIndex(),
            'body'  => [
                '_source'    => [
                    'enabled' => true
                ],
                'properties' => $properties
            ]
        ];

        $this->sql = $body;
        $result = $this->run('indices.putMapping');

        return $result;
    }

    public function deleteIndex()
    {
        $body = [
            'index' => $this->model->getIndex(),
        ];

        $this->sql = $body;
        $result = $this->run('indices.putMapping');

        return $result;
    }

    public function delete($id)
    {
        $body = [
            'index' => $this->model->getIndex(),
            'id'    => $id
        ];
        $this->sql = $body;
        $result = $this->run('indices.delete');
        return $result;
    }


    public function deleteByQuery()
    {
        $query = $this->parseQuery();
        if (empty($query)) { //先不允许全删除
            return [];
        }
        $body = [
            'index' => $this->model->getIndex(),
            'body' => [
                'query'    => $query
            ]
        ];
        $this->sql = $body;
        $result = $this->run('deleteByQuery');
        return $result;
    }

    public function updateSetting($value)
    {
        $body = [
            'index' => $this->model->getIndex(),
            'body' => $value
        ];

        $this->sql = $body;
        $result = $this->run('indices.putSettings');
        return $result;
    }

    public function updateClusterSetting($value)
    {
        $body = [
            'body' => $value

        ];

        $this->sql = $body;
        $result = $this->run('cluster.putSettings');
        return $result;
    }

    public function getSetting()
    {
        $body = [
            'index' => $this->model->getIndex()
        ];
        $this->sql = $body;
        $result = $this->run('indices.getSettings');
        return $result;
    }
}