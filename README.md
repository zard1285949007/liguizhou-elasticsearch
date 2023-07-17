# ES客户端 - hyperf版本

### Composer

```
composer require liguizhou/elasticsearch
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
   OrderModel::query()->whereIn('pid', [1,2,3])->page(1,10); 
```