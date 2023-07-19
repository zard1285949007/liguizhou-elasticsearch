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
            $body['_id'] = $value['id'];
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
                'index' => ['_index' => $this->model->getIndex(), '_type' => '_doc'],
            ];
            if (!empty($value['id'])) {
                $tmp['_id'] = $value['id'];
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

    public function createIndex(array $values)
    {
        $properties = [];
        foreach ($values as $key => $value) {
            $properties[$key] = ['type' => $value];
        }
        $body = [
            'index'    => $this->model->getIndex(),
            'body' => [
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

        $this->sql = $body;
        $result = $this->run('indices.create');

        return $result;
    }

    public function updateIndex(array $values)
    {
        $properties = [];
        foreach ($values as $key => $value) {
            $properties[$key] = ['type' => $value];
        }

        $body = [
            'index'    => $this->model->getIndex(),
            'body' => [
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

}