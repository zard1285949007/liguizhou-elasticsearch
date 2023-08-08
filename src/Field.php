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
            if ($this->judgeOperate($this->fiedString)) { //有四则运算
                $this->dealCommon()
                    ->dealAlias()
                    ->dealOperate();

            } else { //简单聚合
                $this->dealCommon()
                    ->dealAlias()
                    ->dealAgg();
            }
        }
    }

    private function dealOperate()
    {
        $this->aggType = 'raw';
        $this->isAgg = $field['is_agg'] ?? $this->isAgg; //目前四则运算都算聚合吧，太难了

        //切割四则运算拿字符串
        $formula = str_replace(' ', '', $this->field);
        $tokens = preg_split('/([-+*\/()])/', $formula, -1, PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY);
        $operators = ['+', '-', '*', '/', '(', ')'];
        $bucketPath = [];
        $script = '';
        foreach ($tokens as $token) {
            if (in_array($token, $operators)) {
                $script .= $token;
            } else {
                $script .= 'params.' . $token;
                $bucketPath[$token] = $token . '.value';
            }
        }

        //组装sql
        $this->field = [
            'bucket_script' => [
                'buckets_path' => $bucketPath,
                'script'       => $script
            ],
        ];
    }

    private function judgeOperate(string $string): bool
    {
        $operators = ['+', '-', '*', '/'];

        foreach ($operators as $operator) {
            if (strpos($string, $operator) !== false) {
                return true;
            }
        }
        return false;
    }

    /**
     * 对请求字段作公共处理
     * @return $this
     */
    private function dealCommon(): Field
    {
        $this->fiedString = trim(strtolower($this->fiedString));
        return $this;
    }

    /**
     * 对字段的别名作处理
     * @return $this
     */
    private function dealAlias(): Field
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
    private function dealAgg(): Field
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