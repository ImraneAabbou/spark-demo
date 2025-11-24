# Spark: Transformations vs Actions

**Transformation vs Action:**  
- **Transformation:** defines a **logical operation** on a DataFrame/RDD (e.g., `filter`, `select`), but **does not execute** immediately.  
- **Action:** triggers **execution** and returns a result or performs a side effect (e.g., `count`, `show`, `collect`).  

**Why Lazy Evaluation:**  
- Spark uses **lazy evaluation** to **optimize the query plan**, avoid redundant computation, and **combine multiple transformations** into fewer stages for efficiency.

---

# When to Use Actions

- **collect():** returns **all rows** to the driver; use for **small datasets** only and it is considered Dangerous when using large datasets can **Out of memory crash** the driver.  
- **take(n):** returns **first n rows** to the driver; safe for sampling or inspecting top rows.  
- **show(n):** prints **rows to console** without collecting all data; ideal for **quick inspection and debugging**.
