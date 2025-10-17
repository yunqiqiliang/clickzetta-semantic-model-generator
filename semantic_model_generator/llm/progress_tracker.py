"""
Progress tracking system for semantic model enrichment process.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional


class EnrichmentStage(str, Enum):
    """Enumeration of enrichment stages for progress tracking."""

    METADATA_FETCH = "metadata_fetch"
    TABLE_ENRICHMENT = "table_enrichment"
    MODEL_DESCRIPTION = "model_description"
    MODEL_METRICS = "model_metrics"
    VERIFIED_QUERIES = "verified_queries"
    COMPLETE = "complete"


@dataclass
class ProgressUpdate:
    """Represents a progress update during enrichment."""

    stage: EnrichmentStage
    current_step: int
    total_steps: int
    table_name: Optional[str] = None
    message: str = ""
    percentage: float = 0.0
    details: Optional[Dict[str, Any]] = None


class EnrichmentProgressTracker:
    """
    Tracks and reports progress during semantic model enrichment.

    This class manages progress reporting across multiple enrichment stages,
    calculating overall progress percentage and providing structured updates
    to UI components via callback functions.
    """

    def __init__(
        self, progress_callback: Optional[Callable[[ProgressUpdate], None]] = None
    ):
        """
        Initialize the progress tracker.

        Args:
            progress_callback: Optional callback function to receive progress updates.
        """
        self.progress_callback = progress_callback
        self.current_stage = EnrichmentStage.METADATA_FETCH

        # Weight distribution across stages (should sum to 1.0)
        self.stage_weights = {
            EnrichmentStage.METADATA_FETCH: 0.05,  # 5% - Quick metadata collection
            EnrichmentStage.TABLE_ENRICHMENT: 0.70,  # 70% - Most time-consuming (multiple LLM calls)
            EnrichmentStage.MODEL_DESCRIPTION: 0.05,  # 5% - Single LLM call
            EnrichmentStage.MODEL_METRICS: 0.10,  # 10% - Single LLM call
            EnrichmentStage.VERIFIED_QUERIES: 0.10,  # 10% - Single LLM call + validation
        }

        # Track accumulated progress from completed stages
        self.completed_stage_progress = 0.0

    def update_progress(
        self,
        stage: EnrichmentStage,
        current: int,
        total: int,
        table_name: Optional[str] = None,
        message: str = "",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Update progress for the current enrichment stage.

        Args:
            stage: Current enrichment stage
            current: Current step number (0-based or 1-based)
            total: Total number of steps in this stage
            table_name: Optional table name being processed
            message: Human-readable progress message
            details: Optional additional progress details
        """
        # Update current stage if it has changed
        if self.current_stage != stage:
            self._stage_completed(self.current_stage)
            self.current_stage = stage

        # Calculate overall progress percentage
        percentage = self._calculate_overall_percentage(stage, current, total)

        # Create progress update
        update = ProgressUpdate(
            stage=stage,
            current_step=current,
            total_steps=total,
            table_name=table_name,
            message=message,
            percentage=percentage,
            details=details or {},
        )

        # Send update via callback
        if self.progress_callback:
            try:
                self.progress_callback(update)
            except Exception:
                # Silently ignore callback failures to avoid breaking enrichment
                pass

    def _stage_completed(self, stage: EnrichmentStage) -> None:
        """Mark a stage as completed and update accumulated progress."""
        if stage in self.stage_weights:
            self.completed_stage_progress += self.stage_weights[stage]

    def _calculate_overall_percentage(
        self, stage: EnrichmentStage, current: int, total: int
    ) -> float:
        """
        Calculate overall progress percentage across all stages.

        Args:
            stage: Current stage
            current: Current step in stage
            total: Total steps in stage

        Returns:
            Overall progress percentage (0.0 to 100.0)
        """
        # Get weight for current stage
        stage_weight = self.stage_weights.get(stage, 0.0)

        # Calculate progress within current stage
        if total > 0:
            stage_progress = min(current / total, 1.0) * stage_weight
        else:
            stage_progress = 0.0

        # Total progress = completed stages + current stage progress
        total_progress = self.completed_stage_progress + stage_progress

        # Convert to percentage and ensure bounds
        return min(max(total_progress * 100.0, 0.0), 100.0)

    def mark_complete(self) -> None:
        """Mark the entire enrichment process as complete."""
        self.update_progress(
            stage=EnrichmentStage.COMPLETE,
            current=1,
            total=1,
            message="Enrichment complete",
        )


def create_ui_progress_callback() -> Callable[[ProgressUpdate], None]:
    """
    Create a progress callback suitable for Streamlit UI updates.

    Returns:
        Callback function that formats progress updates for UI display.
    """

    def callback(update: ProgressUpdate) -> None:
        """Format and display progress update in UI."""
        # Build progress message
        stage_labels = {
            EnrichmentStage.METADATA_FETCH: "Collecting metadata",
            EnrichmentStage.TABLE_ENRICHMENT: "Enriching tables",
            EnrichmentStage.MODEL_DESCRIPTION: "Generating model description",
            EnrichmentStage.MODEL_METRICS: "Generating model metrics",
            EnrichmentStage.VERIFIED_QUERIES: "Generating verified queries",
            EnrichmentStage.COMPLETE: "Complete",
        }

        stage_label = stage_labels.get(update.stage, update.stage.value)

        if update.table_name:
            base_message = f"{stage_label} - {update.table_name}"
        else:
            base_message = stage_label

        if update.total_steps > 1:
            base_message += f" ({update.current_step}/{update.total_steps})"

        if update.message:
            full_message = f"{base_message}: {update.message}"
        else:
            full_message = base_message

        # In a real implementation, this would update the Streamlit UI
        # For now, we'll use the existing progress callback mechanism
        print(f"[{update.percentage:.1f}%] {full_message}")

    return callback
