<?php

declare(strict_types=1);

namespace Liguizhou\Elasticsearch;

class Field
{
    const AGG_TYPES = ['max', 'min', 'avg', 'sum', 'count'];

    public $field = '';

    public string $aliasField = '';

    public string $aggType = '';

    public int $isAgg = 0;

    public string $fiedString = '';

    public function __construct($field)
    {
        if (is_array($field)) { //数组为原生数据查询
            $this->aggType = 'raw';
            $this->isAgg = $field['is_agg'] ?? $this->isAgg;
            $this->aliasField = $field['alias_field'] ?? $this->aliasField;
            $this->field = $field['field'] ?? [];
        } else {
            $this->fiedString = $field;
            $this->dealCommon()
                ->dealAlias()
                ->dealAgg();
        }
    }

    /**
     * 对请求字段作公共处理
     * @return $this
     */
    protected function dealCommon(): Field
    {
        $this->fiedString = trim(strtolower($this->fiedString));
        return $this;
    }

    /**
     * 对字段的别名作处理
     * @return $this
     */
    public function dealAlias(): Field
    {
        $match = explode(' as ', $this->fiedString);
        if (count($match) == 1) {
            $this->field = $this->aliasField = $this->fiedString;
        } else {
            $this->field = trim($match[0]);
            $this->aliasField = trim($match[1]);
        }

        return $this;
    }

    /**
     * 对聚合作处理
     * @return $this
     */
    public function dealAgg(): Field
    {
        if (!empty($this->field)) {
            $pattern = "/\((.*?)\)/";
            $patternAgg = "/(.*?)\(/";
            if (preg_match($pattern, $this->field, $matches) && preg_match($patternAgg, $this->field, $aggMatches)) {
                $aggField = $matches[1];
                $aggType = $aggMatches[1];
                if (in_array($aggType, self::AGG_TYPES)) {
                    $this->field = $aggField;
                    $this->aggType = $aggType;
                    $this->isAgg = 1;
                }
            }
        }

        return $this;
    }
}