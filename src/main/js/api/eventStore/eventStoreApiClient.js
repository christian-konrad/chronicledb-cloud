class EventStoreApiClient {

    constructor({ type='replicated'}) {
        this.type = type;
        this.path = `/api/event-store${type === 'embedded' ? '/embedded' : ''}`;
    }

    async fetchEventStreamNames() {
        const response = await fetch(`${this.path}/streams`);
        return response.json();
    }

    async fetchEventStreamInfo(streamName) {
        const response = await fetch(`${this.path}/streams/${streamName}/info`);
        return response.json();
    }

    async pushEvents(streamName, events) {
        const response = await fetch(`${this.path}/streams/${streamName}/events`, {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ events })
        });
        return response;
    }

    async pushEvent(streamName, event) {
        return this.pushEvents(streamName, [event]);
    }


    async getAggregate({ streamName, aggregateType, attributeName="ALL", range={} }={}) {
        // TODO validate params
        let path = `${this.path}/streams/${streamName}/aggregates/${aggregateType}/${attributeName}`;
        if (range.lower != null && range.upper != null) {
            const queryParams = new URLSearchParams({
                rangeStart: range.lower,
                rangeEnd: range.upper
            });
            if (range.lowerInclusive != null) queryParams.append("lowerInclusive", range.lowerInclusive);
            if (range.upperInclusive != null) queryParams.append("lowerInclusive", range.upperInclusive);
            path = `${path}?${queryParams}`;
        }

        const response = await fetch(path);
        return response.text();
    }

}

export default EventStoreApiClient;