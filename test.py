from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

# Завдання 1

# Завантажуємо датасет
users_df = spark.read.csv('/Users/eugenechurylov/PycharmProjects/pythonProject1/.venv/users.csv', header=True)
print("users")
users_df.show(5)

purchases_df = spark.read.csv('/Users/eugenechurylov/PycharmProjects/pythonProject1/.venv/purchases.csv', header=True)
print("purchases")
purchases_df.show(5)

products_df = spark.read.csv('/Users/eugenechurylov/PycharmProjects/pythonProject1/.venv/products.csv', header=True)
print("products")
products_df.show(5)

# Завдання 2

cleaned_users_df = users_df.dropna()
cleaned_purchases_df = purchases_df.dropna()
cleaned_products_df = products_df.dropna()

# Кількість рядків після очищення users
cleaned_users_df_count = cleaned_users_df.count()

# Перевіряємо, чи стало менше рядків
print(f"Кількість рядків до очищення users: {users_df.count()}")
print(f"Кількість рядків після очищення users: {cleaned_users_df.count()}")

# Кількість рядків після очищення purchases
cleaned_purchases_df_count = cleaned_purchases_df.count()

# Перевіряємо, чи стало менше рядків
print(f"Кількість рядків до очищення purchases: {purchases_df.count()}")
print(f"Кількість рядків після очищення purchases: {cleaned_purchases_df.count()}")

# Кількість рядків після очищення products
cleaned_products_df_count = cleaned_products_df.count()

# Перевіряємо, чи стало менше рядків
print(f"Кількість рядків до очищення products: {products_df.count()}")
print(f"Кількість рядків після очищення products: {cleaned_products_df.count()}")
print("")

# Завдання 3

# З'єднуємо DataFrame purchases та products за product_id
merged_purchases_products_df = cleaned_purchases_df.join(cleaned_products_df, "product_id")

# Додаємо стовпець з обчисленою сумою покупки
merged_purchases_products_df = merged_purchases_products_df.withColumn("total_price", col("price") * col("quantity"))

# Групуємо за категорією та обчислюємо загальну суму
category_total = merged_purchases_products_df.groupBy("category").sum("total_price").withColumnRenamed("sum(total_price)", "total_price")

# Виводимо результат
print("Загальна сума покупок за кожною категорією продуктів")
category_total.show()

# Завдання 4

# З'єднуємо таблиці purchases, products та users
merged_purchases_products_users_df = cleaned_purchases_df \
    .join(cleaned_products_df, "product_id") \
    .join(cleaned_users_df, "user_id")

# Фільтруємо користувачів віком від 18 до 25 років
filtered_df = merged_purchases_products_users_df.filter((F.col("age") >= 18) & (F.col("age") <= 25))

# Додаємо стовпець з обчисленою сумою покупки
filtered_df = filtered_df.withColumn("total_price", F.col("price") * F.col("quantity"))

# Групуємо за категорією та обчислюємо загальну суму
category_total_age_filtered = filtered_df \
    .groupBy("category") \
    .sum("total_price") \
    .withColumnRenamed("sum(total_price)", "total_price") \
    .orderBy("total_price", ascending=False)

# Виводимо результат
print("Сума покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.")
category_total_age_filtered.show()

# Завдання 5

# Групуємо за категорією та обчислюємо суму покупок для кожної категорії
category_total = filtered_df.groupBy("category").sum("total_price").withColumnRenamed("sum(total_price)", "total_price")

# Обчислюємо сумарну суму витрат для всіх категорій
total_spent = filtered_df.agg(F.sum("total_price").alias("total_spent")).collect()[0]["total_spent"]

# Додаємо стовпець із часткою покупок кожної категорії від загальної суми
category_share = category_total.withColumn("share_of_total", F.col("total_price") / total_spent)
category_share = category_share.withColumn("share_of_total_percentage", F.col("share_of_total") * 100)

# Виводимо результат
print("Частка покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років")
category_share.show()

# Завдання 6

# Сортуємо за відсотком витрат у спадаючому порядку
sorted_category_share = category_share.orderBy(F.col("share_of_total_percentage"), ascending=False)

# Вибираємо перші 3 категорії з найвищим відсотком
top_3_categories = sorted_category_share.limit(3)

# Виводимо результат
print("3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років")
top_3_categories.show()

