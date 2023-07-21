# ES客户端 - hyperf版本

### Composer

```
composer require liguizhou/elasticsearch
```

```
需要注意：使用es时，需要设置max_result_window，search.max_buckets
测试的es设置为1百万
```

### Model

* index 相当于mysql中的表

```php
<?php

declare(strict_types=1);

namespace App\EsModel;

use Liguizhou\Elasticsearch\Model;

class OrderModel extends Model
{
    /**
     * 索引
     * */
    protected $index = 'order';
   
}
```

### 查询

```php
<?php
   OrderModel::query()->where('pid', '1')->get()->toArray();      
   OrderModel::query()->where('pid', 'in', [1,2,3])->groupBy(['pid'])->orderBy(['pid'])->get()->toArray();   
   OrderModel::query()->whereIn('pid', [1,2,3])->offset(0)->limit(10)->paginate(); 
```