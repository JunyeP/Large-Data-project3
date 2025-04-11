# Large Scale Data Processing: Project 3
## Problem 1

| Graph file               | MIS file                    | Is an MIS? |
| ------------------------ | --------------------------- | ---------- |
| small_edges.csv          | small_edges_MIS.csv         | Yes        |
| small_edges.csv          | small_edges_non_MIS.csv     | No         |
| line_100_edges.csv       | line_100_MIS_test_1.csv     | Yes        |
| line_100_edges.csv       | line_100_MIS_test_2.csv     | No         |
| twitter_10000_edges.csv  | twitter_10000_MIS_test_1.csv| No         |
| twitter_10000_edges.csv  | twitter_10000_MIS_test_2.csv| Yes        |

---

##  Problem 2


| Graph file               | Iterations | Time  |
| ------------------------|------------|-------|
| small_edges.csv         | 2          | 4s    |
| line_100_edges.csv      | 3          | 4s    |
| twitter_100_edges.csv   | 3          | 4s    |
| twitter_1000_edges.csv  | 3          | 5s    |
| twitter_10000_edges.csv | 4          | 6s    |

---

##  Problem 3

### 3x4 vCPU

- **Total Vertices:** 11,316,811  
- **Total Execution Time:** 645,698 ms (646s)  
- **Total Iterations:** 6  
- **Vertices in MIS:** 11,172,867 (99%)

#### Iteration Details

| Iteration | Undecided Vertices | Time (ms) |
|-----------|--------------------|-----------|
| 1         | 6,702,534          | 332       |
| 2         | 34,733             | 288       |
| 3         | 366                | 295       |
| 4         | 3                  | 222       |
| 5         | 0                  | 296       |
| 6         | 0                  | 198       |

---

### 4x2 vCPU

- **Total Vertices:** 11,316,811  
- **Total Execution Time:** 1,376,749 ms (1377s)  
- **Total Iterations:** 5  
- **Vertices in MIS:** 11,165,577 (99%)

#### Iteration Details

| Iteration | Undecided Vertices | Time (ms) |
|-----------|--------------------|-----------|
| 1         | 6,792,869          | 640       |
| 2         | 23,119             | 653       |
| 3         | 55                 | 392       |
| 4         | 0                  | 449       |
| 5         | 0                  | 289       |

---

### 2x2 vCPU

- **Total Vertices:** 11,316,811  
- **Total Execution Time:** 2,368,258 ms (2369s)  
- **Total Iterations:** 5  
- **Vertices in MIS:** 11,168,565 (99%)

#### Iteration Details

| Iteration | Undecided Vertices | Time (ms) |
|-----------|--------------------|-----------|
| 1         | 6,400,967          | 468       |
| 2         | 13,197             | 475       |
| 3         | 10                 | 530       |
| 4         | 0                  | 449       |
| 5         | 0                  | 509       |
