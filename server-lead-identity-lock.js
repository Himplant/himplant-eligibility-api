import "dotenv/config";
import express from "express";
import cors from "cors";
import fetch from "node-fetch";

const originalPort = process.env.PORT || "10000";
const externalPort = Number(originalPort) || 10000;
const internalPort = Number(process.env.INTERNAL_SERVER_PORT || externalPort + 1);

const ALLOWED_ORIGINS = [
  "https://eligibility.himplant.com",
  "https://himplant.com",
 