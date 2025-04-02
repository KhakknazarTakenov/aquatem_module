# Use a specific, stable Node.js version (LTS recommended)
FROM node:20-slim

# Set environment variables correctly
ENV NODE_ENV=development

# Set the working directory
WORKDIR /express-docker

# Install required dependencies before running npm install
RUN apt-get update && apt-get install -y \
    nano \
    python3 \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy package.json and package-lock.json first for better caching
COPY package*.json ./

# Install dependencies before copying the whole project
RUN npm install

# Now copy the entire project
COPY . .

# Expose the port
EXPOSE 1328

# Start the application
CMD [ "node", "index.js" ]