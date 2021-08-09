class ApiClient {
    static async fetchSystemInfo() {
        const response = await fetch('/api/sys-info');
        return response.json();
    }

    static async fetchReplicatedCounter() {
        const response = await fetch('/api/counter/replicated');
        return response.json();
    }
}

export default ApiClient;