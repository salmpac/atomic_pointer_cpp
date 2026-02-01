cmake_minimum_required(VERSION 3.13)

project(olymp)

set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_CXX_STANDARD 20)

add_executable(olymp main.cpp)

# Дефолтные флаги для олимпиад
# ВАЖНО: Thread Sanitizer несовместим с Address Sanitizer!
# Используйте либо один, либо другой

# Вариант 1: Address Sanitizer + UndefinedBehavior Sanitizer (текущий)
# target_compile_options(olymp PRIVATE -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer -g)
# target_link_options(olymp PRIVATE -fsanitize=address -fsanitize=undefined)

# Вариант 2: Thread Sanitizer (для поиска data races)
# target_compile_options(olymp PRIVATE -fsanitize=thread -fno-omit-frame-pointer -g)
# target_link_options(olymp PRIVATE -fsanitize=thread)

# Вариант 3: Release с максимальной оптимизацией (для перфтестов)
target_compile_options(olymp PRIVATE -O3 -march=native -DNDEBUG -flto)
target_link_options(olymp PRIVATE -O3 -flto)

# Дополнительные полезные флаги для дебага
# target_compile_options(olymp PRIVATE -O1 -fno-optimize-sibling-calls)
