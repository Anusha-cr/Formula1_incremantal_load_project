# Databricks notebook source
# Goal: calculate the average of numbers
numbers = [10, 20, 30, 40, 50]

# Bug: dividing by a fixed number instead of the length of the list
total = sum(numbers)
average = total / len(numbers)   # ❌ Wrong: should be len(numbers)

print("Average is:", average)

