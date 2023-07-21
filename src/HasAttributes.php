<?php

declare(strict_types=1);
/**
 * 属性
 */

namespace Liguizhou\Elasticsearch;

trait HasAttributes
{
    /**
     * The model's attributes.
     *
     * @var array
     */
    private $attributes = [];

    /**
     * The model attribute's original state.
     *
     * @var array
     */
    private $original = [];
    /**
     * The attributes that should be cast.
     *
     * @var array
     */
    protected $casts = [];

    /**
     * The built-in, primitive cast types supported.
     *
     * @var array
     */
    protected static $primitiveCastTypes = [
        'float'   => 0.00,
        'int'     => 0,
        'integer' => 0,
    ];

    protected function initData(): void
    {
        $this->setOriginal([]);
        $this->setAttributes([]);
    }

    /**
     * @return array
     */
    public function getAttributes(): array
    {
        return $this->attributes;
    }

    /**
     * @param array $attributes
     */
    public function setAttributes(array $attributes, $fields = []): void
    {
        $fields = empty($fields) ? array_keys($attributes) : $fields;

        //补全数据，因为有些文档没有设置这个字段时，不会查出来这个字段
        foreach ($fields as $vField) {
            //获取数据默认值和数据类型
            $defaultValue = '';
            $type = '';
            if (isset($this->casts[$vField])) {
                $type = $this->casts[$vField];
                $defaultValue = self::$primitiveCastTypes[$type] ?? '';
            }
            //补全数据和格式化数据
            if (!isset($attributes[$vField])) {
                $attributes[$vField] = $defaultValue; //给默认值
            } else {
                if (!empty($type)) {
                    switch ($type) {
                        case 'int':
                        case 'integer':
                            $attributes[$vField] = intval($attributes[$vField]);
                            break;
                        case 'float':
                            $attributes[$vField] = floatval($attributes[$vField]);
                            break;
                    }
                }
            }

            //设置变量
            $this->{$vField} = $attributes[$vField];
        }

        $this->attributes = $attributes;
    }

    /**
     * @return array
     */
    public function getOriginal(): array
    {
        return $this->original;
    }

    /**
     * @param array $original
     */
    public function setOriginal(array $original): void
    {
        $this->original = $original;
    }

    /**
     * @return array
     */
    public function getCasts(): array
    {
        return $this->casts;
    }

    /**
     * @param array $casts
     */
    public function setCasts(array $casts): void
    {
        $this->casts = $casts;
    }

    public function toArray(): array
    {
        return $this->getAttributes();
    }

    /**
     * Convert the object into something JSON serializable.
     */
    public function jsonSerialize(): array
    {
        return $this->toArray();
    }

    /**
     * Convert the object to its JSON representation.
     */
    public function toJson(int $options = 0): string
    {
        return json_encode($this->jsonSerialize(), $options);
    }

    /**
     * Convert the model to its string representation.
     */
    public function __toString(): string
    {
        return $this->toJson();
    }
}
