from app.traffic_repository import TrafficRepository

class TrafficAnalyticsService:
    def __init__(self):
        self.repository = TrafficRepository()

    def executive_dashboard(self) -> dict:
        """
        Gathers and bundles high-level security & traffic metrics
        for executive or dashboard consumption.
        """
        return {
            "top_rejects": self.repository.top_rejects().to_dicts(),
            "monthly_traffic": self.repository.monthly().to_dicts(),
        }
